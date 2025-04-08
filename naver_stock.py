import os
import io
import re
import csv
import time
import requests
import pandas as pd
from openai import OpenAI
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader

GITHUB_CSV_URL = f"https://raw.githubusercontent.com/lyh9003/stock_report/main/reports.csv?nocache={int(time.time())}"


# OpenAI API 키 설정: 환경변수 OPENAI_API_KEY가 반드시 올바르게 설정되어 있어야 합니다.
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)


def is_format_line(line):
    """
    줄 전체가 서식용 기호('-','|','=','_')만으로 구성되어 있다면 True를 반환합니다.
    """
    line_stripped = line.strip()
    if not line_stripped:
        return False  # 빈 줄은 그대로 둡니다.
    formatting_chars = set("-|=_")
    return all(c in formatting_chars or c.isspace() for c in line_stripped)

def clean_text(text):
    """
    PDF 텍스트에서 모든 종류의 줄바꿈 및 제어문자(\r, \n, \x0b, \x0c, U+2028, U+2029, 탭)를 공백으로 치환합니다.
    """
    cleaned = re.sub(r'[\r\n\x0b\x0c\u2028\u2029\t]+', ' ', text)
    return cleaned.strip()

def classify_title(prompt):
    """
    경영사장단 보고용 증권레포트 '본문 전체'를 아래 양식에 맞게 요약합니다.
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": (
                    """
                    당신은 반도체기업 사장단에게 보고할 증권레포트 요약 전문가입니다. 
                    전체 보고서 본문을 읽고 아래 양식에 맞게 재무 성과, 시장 동향, 주요 리스크와 기회, 
                    기타 전략적 인사이트를 요약해 주세요. 글자수는 레포트가 짧으면 400자 내외, 길면 1000자 내외로 해주세요.
                    보고서를 기반으로 요약해야 합니다. **는 쓰지 말아주고 들여쓰기도 하지 말아줘.
                    [보고서 요약 양식]
                    1. 시장 동향:
                    - 현재 시장 상황 및 변화
                    2. 주요 리스크:
                    - 잠재적 위험 요소
                    3. 주요 기회:
                    - 향후 성장 및 기회
                    4. 기타 전략적 인사이트:
                    - 추가적으로 고려할 사항
                    """
                )},
                {"role": "user", "content": prompt}
            ]
        )
        result = response.choices[0].message.content.strip()
    except Exception as e:
        print(f"요약 생성 중 오류 발생: {e}")
        result = ""
    return result

def one_line_summary(prompt):
    """
    증권레포트 본문을 한 문장으로 요약합니다.
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "당신은 반도체기업 사장단에게 보고할 증권레포트 1줄 요약 전문가입니다. 다음 보고서 본문을 한 문장으로 요약해 주세요."
                },
                {"role": "user", "content": prompt}
            ]
        )
        result = response.choices[0].message.content.strip()
    except Exception as e:
        print(f"1줄 요약 생성 중 오류 발생: {e}")
        result = ""
    return result

# 크롤링할 사이트 URL 목록 및 CSV 파일 경로
urls = [
    'https://finance.naver.com/research/industry_list.naver?keyword=&brokerCode=&writeFromDate=&writeToDate=&searchType=upjong&upjong=%B9%DD%B5%B5%C3%BC&x=8&y=16',  # 반도체산업레포트
    'https://finance.naver.com/research/company_list.naver?keyword=&brokerCode=&writeFromDate=&writeToDate=&searchType=itemCode&itemName=%BB%EF%BC%BA%C0%FC%C0%DA&itemCode=005930&x=23&y=16',  # 삼성전자레포트
    'https://finance.naver.com/research/company_list.naver?keyword=&brokerCode=&writeFromDate=&writeToDate=&searchType=itemCode&itemName=SK%C7%CF%C0%CC%B4%D0%BDBA&itemCode=000660&x=45&y=28'  # 하이닉스레포트
]
csv_file = "reports.csv"

# 기존 CSV 파일이 있으면 읽고, 없으면 새로 생성
if os.path.exists(csv_file):
    existing_df = pd.read_csv(csv_file)
    existing_links = set(existing_df['link'].tolist())
    index_counter = existing_df['index'].max() + 1 if not existing_df.empty else 1
else:
    existing_df = pd.DataFrame()
    existing_links = set()
    index_counter = 1

session = requests.Session()
new_reports = []  # 신규 보고서 정보를 저장할 리스트

for url in urls:
    print(f"Crawling URL: {url}")
    response = session.get(url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    processed_count = 0  # 각 사이트에서 처리한 PDF 개수를 카운트합니다.
    
    for row in soup.find_all("tr"):
        file_td = row.find("td", class_="file")
        if not file_td:
            continue  # PDF 파일이 없는 행은 건너뜁니다.
        
        tds = row.find_all("td")
        if len(tds) < 5:
            continue

        # 보고서 제목, 증권사, 날짜, PDF 링크 추출
        title_tag = tds[1].find("a")
        report_title = title_tag.get_text(strip=True) if title_tag else ""
        broker_name = tds[2].get_text(strip=True)
        date_td = row.find("td", class_="date")
        date_str = date_td.get_text(strip=True) if date_td else ""
        pdf_a = file_td.find("a")
        pdf_url = pdf_a['href'] if pdf_a and 'href' in pdf_a.attrs else ""

        # 이미 저장된 링크라면 건너뜁니다.
        if pdf_url in existing_links:
            print(f"이미 저장된 보고서입니다: {pdf_url}")
            processed_count += 1
            if processed_count >= 2:
                break
            continue

        # PDF 파일 다운로드 및 텍스트 추출
        pdf_text = ""
        file_size = None  # 파일 크기 (바이트)
        if pdf_url:
            try:
                print(f"PDF 다운로드 중: {pdf_url}")
                pdf_response = session.get(pdf_url)
                pdf_response.raise_for_status()
                file_size = len(pdf_response.content)  # 파일 크기 계산 (바이트)
                pdf_file = io.BytesIO(pdf_response.content)
                reader = PdfReader(pdf_file)
                page_texts = []
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        # 각 페이지를 줄 단위로 분리 후, 서식용 줄 제거
                        lines = page_text.splitlines()
                        filtered_lines = [line.strip() for line in lines if not is_format_line(line)]
                        page_clean_text = " ".join(filtered_lines)
                        page_texts.append(page_clean_text)
                pdf_text = " ".join(page_texts)
                pdf_text = clean_text(pdf_text)
            except Exception as e:
                print(f"PDF 처리 중 오류 발생: {e}")

        # 신규 보고서에 대해서만 요약 실행 (PDF 텍스트가 있을 경우)
        if pdf_text:
            # 전체 보고서 요약 (원래의 요약)
            full_summary_text = classify_title(pdf_text)
            # 1줄 요약
            one_line_text = one_line_summary(pdf_text)
        else:
            full_summary_text = ""
            one_line_text = ""
        
        new_reports.append({
            "index": index_counter,
            "날짜": date_str,
            "증권사": broker_name,
            "레포트제목": report_title,
            "레포트본문전체": pdf_text,
            "전체요약": full_summary_text,
            "1줄 요약": one_line_text,
            "link": pdf_url,
            "파일크기": file_size
        })
        print(f"신규 보고서 {index_counter} 처리 완료: {report_title}")
        index_counter += 1
        
        processed_count += 1
        #if processed_count >= 2: 
        #   break

# 신규 데이터가 있으면 기존 데이터와 합쳐 CSV로 저장 (모든 셀을 큰따옴표로 감쌈)
if new_reports:
    new_df = pd.DataFrame(new_reports)
    if not existing_df.empty:
        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        updated_df = new_df

    # 날짜 컬럼을 datetime 타입으로 변환 (입력 형식은 '년.월.일' 이므로 format='%y.%m.%d' 사용)
    updated_df['날짜'] = pd.to_datetime(updated_df['날짜'], format='%y.%m.%d', errors='coerce')
    # 날짜 기준 내림차순 정렬 (데이터프레임에 저장될 때는 '년-월-일' 포맷이 기본 출력입니다)
    updated_df = updated_df.sort_values(by='날짜', ascending=False)

    # 최종 CSV 파일 저장 전 "index" 칼럼 삭제
    if 'index' in updated_df.columns:
        updated_df = updated_df.drop(columns=['index'])
        
    updated_df.to_csv(csv_file, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
    print(f"CSV 파일 저장 완료: {csv_file} (추가된 보고서 수: {len(new_reports)})")
else:
    print("새로운 데이터가 없습니다.")
