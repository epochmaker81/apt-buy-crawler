import os
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import xml.etree.ElementTree as ET
import time
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 설정 변수 ---
SERVICE_KEY = os.getenv('SERVICE_KEY')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_NAME = '전국 아파트 매매 실거래가_누적'
LAWD_CODE_FILE = 'lawd_code.csv'
BASE_URL = 'https://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'

# 실행 모드 설정 (환경 변수로 제어)
RUN_MODE = os.getenv('RUN_MODE', 'QUICK')  # QUICK, FULL, TEST

# 날짜 설정
today_kst = datetime.utcnow() + timedelta(hours=9)

if RUN_MODE == 'TEST':
    # 테스트 모드: 최근 1개월, 서울 지역만
    MONTHS_TO_FETCH = [today_kst.strftime('%Y%m')]
    TARGET_REGIONS = ['11110', '11140', '11170']  # 서울 일부 지역만
elif RUN_MODE == 'QUICK':
    # 빠른 모드: 최근 1개월, 주요 도시만
    MONTHS_TO_FETCH = [today_kst.strftime('%Y%m')]
    TARGET_REGIONS = None  # 파일에서 읽되 주요 도시만 필터링
else:
    # 전체 모드: 3개월, 전국
    MONTHS_TO_FETCH = []
    for i in range(3):
        target_date = today_kst - relativedelta(months=i)
        MONTHS_TO_FETCH.append(target_date.strftime('%Y%m'))
    TARGET_REGIONS = None

def get_google_creds():
    """Google 인증 정보 가져오기"""
    if GOOGLE_CREDENTIALS_JSON is None:
        logger.error("GOOGLE_CREDENTIALS_JSON이 설정되지 않았습니다.")
        return None
    try:
        return json.loads(GOOGLE_CREDENTIALS_JSON)
    except json.JSONDecodeError:
        logger.error("GOOGLE_CREDENTIALS_JSON이 유효하지 않습니다.")
        return None

def get_lawd_codes(filepath):
    """지역 코드 파일 읽기 (필터링 포함)"""
    try:
        df = pd.read_csv(filepath)
        codes = df['code'].astype(str).tolist()
        
        if TARGET_REGIONS:
            # 특정 지역만 선택
            codes = [c for c in codes if c in TARGET_REGIONS]
        elif RUN_MODE == 'QUICK':
            # 주요 도시만 선택 (서울, 부산, 대구, 인천, 광주, 대전, 울산, 세종)
            major_city_prefixes = ['11', '26', '27', '28', '29', '30', '31', '36']
            codes = [c for c in codes if any(c.startswith(p) for p in major_city_prefixes)]
        
        logger.info(f"처리할 지역 코드: {len(codes)}개")
        return codes
    except Exception as e:
        logger.error(f"지역 코드 파일 읽기 오류: {e}")
        return []

def fetch_data_simple(lawd_cd, deal_ymd, service_key):
    """단순화된 데이터 가져오기 (재시도 최소화)"""
    params = {
        'serviceKey': service_key,
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ymd,
        'pageNo': '1',
        'numOfRows': '5000'
    }
    
    # 단 2번만 시도
    for attempt in range(2):
        try:
            # 세션 대신 직접 요청 (연결 재사용 없음)
            response = requests.get(
                BASE_URL,
                params=params,
                timeout=30,  # 타임아웃 단축
                verify=False
            )
            
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                result_code = root.find('header/resultCode')
                
                if result_code is not None and result_code.text == '00':
                    items_element = root.find('body/items')
                    if items_element:
                        return [
                            {child.tag: (child.text.strip() if child.text else '')
                             for child in item}
                            for item in items_element.findall('item')
                        ]
                elif result_code is not None and result_code.text == '99':
                    return []  # 데이터 없음
            
            if attempt == 0:
                time.sleep(random.uniform(1, 3))  # 랜덤 대기
                
        except Exception as e:
            if attempt == 1:
                logger.debug(f"실패: {lawd_cd} - {str(e)[:30]}")
            time.sleep(2)
    
    return []

def process_batch_parallel(lawd_codes, deal_ymd, service_key, max_workers=5):
    """병렬 처리로 속도 향상"""
    all_data = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 작업 제출
        future_to_code = {
            executor.submit(fetch_data_simple, code, deal_ymd, service_key): code
            for code in lawd_codes
        }
        
        # 완료된 작업 처리
        completed = 0
        total = len(lawd_codes)
        
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            completed += 1
            
            try:
                data = future.result()
                if data:
                    all_data.extend(data)
                    logger.info(f"[{completed}/{total}] {code} - {len(data)}건")
                else:
                    logger.debug(f"[{completed}/{total}] {code} - 데이터 없음")
            except Exception as e:
                logger.error(f"[{completed}/{total}] {code} - 오류: {str(e)[:50]}")
            
            # 진행률 표시
            if completed % 10 == 0:
                logger.info(f"진행률: {completed}/{total} ({completed*100/total:.1f}%)")
    
    return all_data

def create_unique_id(df):
    """고유 ID 생성"""
    if df.empty:
        return df
    
    id_cols = ['거래금액', '년', '월', '일', '전용면적', '지번', '층', 
               '법정동시군구코드', '법정동읍면동코드']
    valid_cols = [col for col in id_cols if col in df.columns]
    
    if valid_cols:
        df['unique_id'] = df[valid_cols].astype(str).agg('_'.join, axis=1)
    return df

def upload_to_sheet(df_new, df_existing, worksheet):
    """Google Sheets에 업로드"""
    if df_new.empty:
        return 0, df_existing
    
    df_new = create_unique_id(df_new)
    
    if not df_existing.empty:
        if 'unique_id' not in df_existing.columns:
            df_existing = create_unique_id(df_existing)
        newly_added = df_new[~df_new['unique_id'].isin(df_existing['unique_id'])].copy()
    else:
        newly_added = df_new.copy()
    
    if newly_added.empty:
        return 0, df_existing
    
    count = len(newly_added)
    df_to_upload = newly_added.drop(columns=['unique_id'], errors='ignore')
    
    try:
        if worksheet.row_count < 2:
            set_with_dataframe(worksheet, df_to_upload, include_index=False)
        else:
            headers = worksheet.row_values(1)
            aligned = pd.DataFrame(columns=headers)
            for col in headers:
                if col in df_to_upload.columns:
                    aligned[col] = df_to_upload[col]
            worksheet.append_rows(aligned.values.tolist(), value_input_option='USER_ENTERED')
        
        df_existing_updated = pd.concat([df_existing, newly_added], ignore_index=True)
        return count, df_existing_updated
    except Exception as e:
        logger.error(f"업로드 오류: {e}")
        return -1, df_existing

def main():
    """메인 함수"""
    start_time = time.time()
    
    # 환경 확인
    if not SERVICE_KEY or not GOOGLE_CREDENTIALS_JSON:
        logger.error("필수 환경 변수가 설정되지 않았습니다.")
        return
    
    logger.info(f"===== 아파트 실거래가 업데이트 시작 =====")
    logger.info(f"실행 모드: {RUN_MODE}")
    logger.info(f"대상 월: {MONTHS_TO_FETCH}")
    
    # 지역 코드 로드
    lawd_codes = get_lawd_codes(LAWD_CODE_FILE)
    if not lawd_codes:
        return
    
    # Google Sheets 초기화
    try:
        creds = get_google_creds()
        if not creds:
            return
        
        gc = gspread.service_account_from_dict(creds)
        
        try:
            sh = gc.open(GOOGLE_SHEET_NAME)
        except gspread.exceptions.SpreadsheetNotFound:
            sh = gc.create(GOOGLE_SHEET_NAME)
            logger.info(f"새 시트 생성: {GOOGLE_SHEET_NAME}")
        
        worksheet = sh.get_worksheet(0)
        
        # 기존 데이터 읽기
        logger.info("기존 데이터 로드 중...")
        existing_records = worksheet.get_all_records()
        df_existing = pd.DataFrame(existing_records)
        if not df_existing.empty:
            df_existing.columns = df_existing.columns.str.strip()
            
    except Exception as e:
        logger.error(f"시트 초기화 오류: {e}")
        return
    
    total_added = 0
    
    # 월별 처리
    for month in MONTHS_TO_FETCH:
        logger.info(f"\n===== {month} 처리 시작 =====")
        month_start = time.time()
        
        # 병렬 처리로 데이터 수집
        if RUN_MODE == 'TEST':
            # 테스트 모드: 순차 처리
            monthly_data = []
            for code in lawd_codes:
                data = fetch_data_simple(code, month, SERVICE_KEY)
                if data:
                    monthly_data.extend(data)
        else:
            # 일반 모드: 병렬 처리
            monthly_data = process_batch_parallel(
                lawd_codes, month, SERVICE_KEY,
                max_workers=3 if RUN_MODE == 'QUICK' else 5
            )
        
        if not monthly_data:
            logger.warning(f"{month}: 수집된 데이터 없음")
            continue
        
        # 데이터프레임 생성 및 업로드
        df_month = pd.DataFrame(monthly_data)
        df_month.columns = df_month.columns.str.strip()
        logger.info(f"{month}: {len(df_month)}건 수집")
        
        added, df_existing = upload_to_sheet(df_month, df_existing, worksheet)
        
        if added > 0:
            total_added += added
            logger.info(f"{month}: {added}건 추가")
        
        logger.info(f"{month} 처리 시간: {time.time() - month_start:.1f}초")
    
    # 완료
    elapsed = time.time() - start_time
    logger.info(f"\n===== 완료 =====")
    logger.info(f"총 {total_added}건 추가")
    logger.info(f"소요 시간: {elapsed//60:.0f}분 {elapsed%60:.0f}초")

if __name__ == '__main__':
    main()
