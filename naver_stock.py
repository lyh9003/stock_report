import os
import io
import re
import csv
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from openai import OpenAI
from PyPDF2 import PdfReader

# ──────────────────────────────────────────────
# 0. 기본 설정
# ──────────────────────────────────────────────
GITHUB_CSV_URL = (
    "https://raw.githubusercontent.com/lyh9003/stock_report/main/reports.csv"
    f"?nocache={int(time.time())}"
)

api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

# ──────────────────────────────────────────────
# 1. 전처리/유틸 함수
# ──────────────────────────────────────────────
def is_format_line(line: str) -> bool:
    """줄 전체가 서식용 기호로만 구성되어 있으면 True"""
    line_stripped = line.strip()
    if not line_stripped:
        return False
    formatting_chars = set("-|=_")
    return all(c in formatting_chars or c.isspace() for c in line_stripped)


def clean_text(text: str) -> str:
    """PDF 텍스트의 모든 줄바꿈·제어문자를 공백으로 치환"""
    return re.sub(r"[\r\n\x0b\x0c\u2028\u2029\t]+", " ", text).strip()


def parse_date(x):
    """'yy.mm.dd' 또는 ISO 'YYYY-MM-DD' 두 형식 모두 처리"""
    if pd.isna(x):
        return pd.NaT
    s = str(x).strip()
    try:
        if "." in s:  # 24.04.30 등
            return pd.to_datetime(s, format="%y.%m.%d", errors="coerce")
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return pd.NaT


# ──────────────────────────────────────────────
# 2. GPT 요약/키워드 함수
# ──────────────────────────────────────────────
def classify_title(prompt):
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "당신은 반도체기업 사장단에게 보고할 증권레포트 요약 전문가이다. "
                        "보고서 본문을 읽고 아래 양식으로 400~1000자 요약하라. "
                        "[양식] 1. 시장 동향: 2. 산업 이슈: 3. 기술 트랜드: "
                        "4. 기타 전략적 인사이트: 5. 주요 키워드:"
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"[GPT] 요약 오류: {e}")
        return ""


def one_line_summary(prompt):
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "당신은 반도체기업 사장단에게 보고할 증권레포트 1줄 요약 전문가이다. "
                        "본문을 한 문장으로 요약하라."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"[GPT] 1줄 요약 오류: {e}")
        return ""


def extract_keywords(summary_text):
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "주어진 요약에서 핵심 키워드 5~10개를 쉼표로 구분하여 출력하라."
                    ),
                },
                {"role": "user", "content": summary_text},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"[GPT] 키워드 오류: {e}")
        return ""


# ──────────────────────────────────────────────
# 3. 크롤링 대상 URL
# ──────────────────────────────────────────────
urls = [
    "https://finance.naver.com/research/industry_list.naver?keyword=&brokerCode=&writeFromDate=&writeToDate=&searchType=upjong&upjong=%B9%DD%B5%B5%C3%BC",
    "https://finance.naver.com/research/company_list.naver?searchType=itemCode&itemName=%BB%EF%BC%BA%C0%FC%C0%DA&itemCode=005930",
    "https://finance.naver.com/research/company_list.naver?searchType=itemCode&itemName=SK%C7%CF%C0%CC%B4%D0%BDBA&itemCode=000660",
]

csv_file = "reports.csv"

# ──────────────────────────────────────────────
# 4. 기존 CSV 로드
# ──────────────────────────────────────────────
if os.path.exists(csv_file):
    existing_df = pd.read_csv(csv_file)
    existing_links = set(existing_df["link"].tolist())
    index_counter = len(existing_df) + 1
else:
    existing_df = pd.DataFrame()
    existing_links = set()
    index_counter = 1

# ──────────────────────────────────────────────
# 5. 크롤링 및 PDF 처리
# ──────────────────────────────────────────────
session = requests.Session()
new_reports = []

for url in urls:
    print(f"[CRAWL] {url}")
    resp = session.get(url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    processed = 0

    for row in soup.find_all("tr"):
        file_td = row.find("td", class_="file")
        if not file_td:
            continue

        # ――― 수정 부분: <a> 태그 및 href 안전 체크 ―――
        pdf_a = file_td.find("a")
        if not pdf_a or not pdf_a.has_attr("href"):
            continue  # 링크 없는 행 건너뜀
        pdf_url = pdf_a["href"]
        # ―――――――――――――――――――――――――――――――――――――――

        if pdf_url in existing_links:
            processed += 1
            if processed >= 2:
                break
            continue

        tds = row.find_all("td")
        if len(tds) < 5:
            continue

        title_tag = tds[1].find("a")
        report_title = title_tag.get_text(strip=True) if title_tag else ""
        broker_name = tds[2].get_text(strip=True)
        date_str = row.find("td", class_="date").get_text(strip=True)

        # PDF 다운로드
        print(f"  └─ PDF 다운로드: {pdf_url}")
        pdf_text, file_size = "", None
        try:
            pdf_resp = session.get(pdf_url)
            pdf_resp.raise_for_status()
            file_size = len(pdf_resp.content)

            reader = PdfReader(io.BytesIO(pdf_resp.content))
            pages = []
            for pg in reader.pages:
                lines = pg.extract_text().splitlines()
                pages.append(
                    " ".join(
                        ln.strip() for ln in lines if not is_format_line(ln)
                    )
                )
            pdf_text = clean_text(" ".join(pages))
        except Exception as e:
            print(f"  └─ PDF 처리 오류: {e}")

        # GPT 요약
        if pdf_text:
            full_summary = classify_title(pdf_text)
            one_line = one_line_summary(full_summary)
            keywords = extract_keywords(full_summary)
        else:
            full_summary = one_line = keywords = ""

        new_reports.append(
            {
                "index": index_counter,
                "날짜": date_str,
                "증권사": broker_name,
                "레포트제목": report_title,
                "레포트본문전체": pdf_text,
                "전체요약": full_summary,
                "1줄 요약": one_line,
                "키워드": keywords,
                "link": pdf_url,
                "파일크기": file_size,
            }
        )
        print(f"  └─ 저장: {index_counter} ({report_title})")
        index_counter += 1
        processed += 1

# ──────────────────────────────────────────────
# 6. CSV 저장
# ──────────────────────────────────────────────
if new_reports:
    new_df = pd.DataFrame(new_reports)
    new_df["날짜"] = new_df["날짜"].apply(parse_date)

    updated_df = (
        pd.concat([existing_df, new_df], ignore_index=True)
        if not existing_df.empty
        else new_df
    )
    updated_df["날짜"] = updated_df["날짜"].apply(parse_date)
    updated_df = updated_df.sort_values(
        "날짜", ascending=False, na_position="last"
    )

    if "index" in updated_df.columns:
        updated_df = updated_df.drop(columns=["index"])

    updated_df.to_csv(
        csv_file, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL
    )
    print(f"[SAVE] {csv_file} (추가 {len(new_reports)}건)")
else:
    print("[INFO] 새 보고서 없음")
