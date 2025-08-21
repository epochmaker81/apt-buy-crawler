import os
import time
import logging
import json
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime
from dateutil.relativedelta import relativedelta
from xml.etree import ElementTree

# --- 1. 로깅 설정 ---
# 로그 파일과 콘솔에 모두 출력하도록 설정
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 파일 핸들러 (debug.log)
file_handler = logging.FileHandler('debug.log')
file_handler.setFormatter(log_formatter)

# 스트림 핸들러 (콘솔)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)

# 로거 객체 생성 및 핸들러 추가
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


# --- 2. 환경 변수 및 설정 불러오기 ---
try:
    SERVICE_KEY = os.environ['SERVICE_KEY']
    GOOGLE_CREDENTIALS_JSON = os.environ['GOOGLE_CREDENTIALS_JSON']
    
    # 자신의 구글 시트 및 워크시트 이름으로 변경하세요.
    GOOGLE_SHEET_NAME = "아파트 실거래가 데이터" 
    WORKSHEET_NAME = "raw_data"

except KeyError as e:
    logger.error(f"필수 환경 변수가 설정되지 않았습니다: {e}")
    exit(1)

API_ENDPOINT = "http://openapi.molit.go.kr/OpenAPI_ToolInstall/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"

# --- 3. 구글 시트 클라이언트 인증 ---
def get_gspread_client():
    """환경 변수에서 구글 인증 정보를 읽어 gspread 클라이언트를 반환합니다."""
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        return gspread.service_account_from_dict(creds_dict)
    except json.JSONDecodeError:
        logger.error("GOOGLE_CREDENTIALS_JSON 형식이 올바르지 않습니다.")
        return None
    except Exception as e:
        logger.error(f"구글 시트 인증 중 오류 발생: {e}")
        return None

# --- 4. API 데이터 요청 및 파싱 함수 ---
def fetch_apt_data(service_key, lawd_cd, deal_ymd):
    """지정된 지역 코드와 계약 월에 해당하는 아파트 매매 실거래가 데이터를 API로부터 가져옵니다."""
    url = f"{API_ENDPOINT}?serviceKey={service_key}&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}&numOfRows=9999"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()  # HTTP 오류 발생 시 예외 발생
        
        root = ElementTree.fromstring(response.content)
        items = []
        for item in root.findall('.//item'):
            data = {child.tag: child.text for child in item}
            items.append(data)
        return items
    except requests.exceptions.RequestException as e:
        logger.warning(f"API 요청 실패 (LAWD_CD: {lawd_cd}, DEAL_YMD: {deal_ymd}): {e}")
        return []
    except ElementTree.ParseError as e:
        logger.warning(f"XML 파싱 실패 (LAWD_CD: {lawd_cd}, DEAL_YMD: {deal_ymd}): {e}")
        logger.debug(f"응답 내용: {response.text}")
        return []

# --- 5. 데이터 정제 함수 ---
def clean_dataframe(df):
    """수집된 데이터프레임을 정제합니다."""
    if df.empty:
        return df
        
    # 거래금액의 쉼표 제거 및 공백 제거 후 숫자형으로 변환
    df['거래금액'] = df['거래금액'].str.replace(',', '').str.strip().astype(int)
    
    # 계약일자 생성
    df['계약년'] = df['년'].astype(str)
    df['계약월'] = df['월'].astype(str).str.zfill(2)
    df['계약일'] = df['일'].astype(str).str.zfill(2)
    df['계약일자'] = pd.to_datetime(df['계약년'] + df['계약월'] + df['계약일'], format='%Y%m%d')

    # 숫자형 데이터 타입 변환 (errors='coerce'는 변환 실패 시 NaT/NaN으로 처리)
    numeric_cols = ['전용면적', '층', '건축년도', '거래금액']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 필요한 컬럼 선택 및 순서 정리
    final_cols = [
        '계약일자', '거래금액', '건축년도', '아파트', '전용면적', '층',
        '법정동', '지역코드', '지번', '해제사유발생일', '해제여부'
    ]
    
    # df에 존재하는 컬럼만 필터링하여 최종 컬럼 순서를 보장
    existing_cols = [col for col in final_cols if col in df.columns]
    
    return df[existing_cols]


# --- 6. 메인 실행 로직 ---
def main():
    """메인 실행 함수"""
    logger.info("스크립트 실행 시작")
    
    # 데이터 수집 대상 월 계산 (지난달, 지지난달)
    today = datetime.now()
    target_months = [
        (today - relativedelta(months=1)).strftime('%Y%m'),  # 지난달
        (today - relativedelta(months=2)).strftime('%Y%m')   # 지지난달
    ]
    logger.info(f"데이터 수집 대상 월: {', '.join(target_months)}")

    # 지역 코드 불러오기
    try:
        lawd_codes_df = pd.read_csv('lawd_code.csv')
    except FileNotFoundError:
        logger.error("lawd_code.csv 파일을 찾을 수 없습니다.")
        return
        
    # 테스트 모드 확인
    is_test_mode = os.environ.get('RUN_MODE') == 'TEST'
    if is_test_mode:
        lawd_codes_df = lawd_codes_df.head(3) # 테스트 시 3개 지역만
        logger.info("--- 테스트 모드로 실행됩니다. (3개 지역만 처리) ---")

    all_transactions = []
    
    # 지정된 모든 월에 대해 데이터 수집
    for deal_ymd in target_months:
        logger.info(f"===== {deal_ymd} 월 데이터 수집 시작 =====")
        total_codes = len(lawd_codes_df)
        
        # 모든 지역 코드에 대해 데이터 수집
        for i, row in lawd_codes_df.iterrows():
            lawd_cd = row['code']
            logger.info(f"[{i+1}/{total_codes}] 지역 코드 {lawd_cd} 데이터 수집 중... (대상월: {deal_ymd})")
            
            transactions = fetch_apt_data(SERVICE_KEY, lawd_cd, deal_ymd)
            if transactions:
                all_transactions.extend(transactions)
                logger.info(f"  -> {len(transactions)}건 데이터 발견")
            
            time.sleep(0.3) # API 과부하 방지를 위한 지연

    if not all_transactions:
        logger.warning("수집된 데이터가 없습니다. 스크립트를 종료합니다.")
        return

    logger.info(f"총 {len(all_transactions)}건의 거래 데이터를 수집했습니다.")

    # 데이터프레임으로 변환 및 정제
    raw_df = pd.DataFrame(all_transactions)
    final_df = clean_dataframe(raw_df)
    logger.info(f"데이터 정제 완료. 최종 데이터: {final_df.shape[0]} 행, {final_df.shape[1]} 열")
    
    # 구글 시트에 업데이트
    gc = get_gspread_client()
    if not gc:
        logger.error("구글 시트 클라이언트를 가져올 수 없어 업데이트를 중단합니다.")
        return

    try:
        logger.info(f"'{GOOGLE_SHEET_NAME}' 스프레드시트에 데이터 업데이트 시작...")
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"'{WORKSHEET_NAME}' 워크시트를 찾을 수 없어 새로 생성합니다.")
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1, cols=1)

        worksheet.clear() # 기존 데이터 모두 삭제
        set_with_dataframe(worksheet, final_df) # 새로운 데이터로 덮어쓰기
        logger.info("구글 시트 업데이트 성공!")

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"스프레드시트 '{GOOGLE_SHEET_NAME}'을 찾을 수 없습니다. 파일 이름과 공유 설정을 확인하세요.")
    except Exception as e:
        logger.error(f"구글 시트 업데이트 중 오류 발생: {e}")

    logger.info("스크립트 실행 완료")

if __name__ == "__main__":
    main()
