import os
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import xml.etree.ElementTree as ET
import time
import ssl
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CustomHttpAdapter(HTTPAdapter):
    """SSL 및 재시도 정책을 포함한 커스텀 HTTP 어댑터"""
    def __init__(self, *args, **kwargs):
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE  # GitHub Actions 환경을 위해 일시적으로 SSL 검증 비활성화
        super().__init__(*args, **kwargs)
        
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs['ssl_context'] = self.ssl_context
        return super().init_poolmanager(connections, maxsize, block, **pool_kwargs)

# --- 설정 변수 ---
SERVICE_KEY = os.getenv('SERVICE_KEY')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_NAME = '전국 아파트 매매 실거래가_누적'
LAWD_CODE_FILE = 'lawd_code.csv'

# 운영 계정용 URL
BASE_URL = 'https://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'

# 날짜 자동 생성 (최근 3개월)
today_kst = datetime.utcnow() + timedelta(hours=9)
MONTHS_TO_FETCH = []
for i in range(3):
    target_date = today_kst - relativedelta(months=i)
    MONTHS_TO_FETCH.append(target_date.strftime('%Y%m'))

def create_session_with_retry():
    """재시도 정책이 포함된 세션 생성"""
    session = requests.Session()
    
    # 재시도 전략 설정
    retry_strategy = Retry(
        total=5,  # 총 재시도 횟수
        backoff_factor=2,  # 재시도 간 대기 시간 증가 계수
        status_forcelist=[429, 500, 502, 503, 504],  # 재시도할 HTTP 상태 코드
        allowed_methods=["GET", "POST"]  # 재시도 허용 메서드
    )
    
    adapter = CustomHttpAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # 헤더 설정
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/xml, text/xml, */*',
        'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
        'Connection': 'keep-alive'
    }
    session.headers.update(headers)
    
    return session

def get_google_creds():
    """Google 인증 정보 가져오기"""
    if GOOGLE_CREDENTIALS_JSON is None:
        logger.error("오류: GitHub Secrets에 'GOOGLE_CREDENTIALS_JSON'이 설정되지 않았습니다.")
        return None
    try:
        return json.loads(GOOGLE_CREDENTIALS_JSON)
    except json.JSONDecodeError:
        logger.error("오류: 'GOOGLE_CREDENTIALS_JSON' Secret의 값이 유효한 JSON 형식이 아닙니다.")
        return None

def get_lawd_codes(filepath):
    """지역 코드 파일 읽기"""
    try:
        df = pd.read_csv(filepath)
        codes = df['code'].astype(str).tolist()
        logger.info(f"총 {len(codes)}개의 지역 코드를 불러왔습니다.")
        return codes
    except FileNotFoundError:
        logger.error(f"오류: {filepath} 파일을 찾을 수 없습니다.")
        return []
    except Exception as e:
        logger.error(f"지역 코드 파일 읽기 오류: {e}")
        return []

def fetch_data_for_region(session, lawd_cd, deal_ymd, service_key, max_retries=5):
    """특정 지역의 데이터 가져오기 (개선된 에러 처리)"""
    params = {
        'serviceKey': service_key,
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ymd,
        'pageNo': '1',
        'numOfRows': '5000'
    }
    
    for attempt in range(max_retries):
        try:
            # 타임아웃 증가 및 verify=False 추가 (SSL 문제 해결)
            response = session.get(
                BASE_URL, 
                params=params, 
                timeout=120,  # 타임아웃 증가
                verify=False  # SSL 검증 비활성화 (필요시)
            )
            response.raise_for_status()
            
            # XML 파싱
            all_items = []
            root = ET.fromstring(response.content)
            
            # 응답 코드 확인
            result_code_element = root.find('header/resultCode')
            if result_code_element is None:
                logger.warning(f"지역코드 {lawd_cd}: 응답 헤더가 없습니다.")
                return []
            
            result_code = result_code_element.text
            if result_code == '00':  # 정상
                items_element = root.find('body/items')
                if items_element is None:
                    return []
                
                for item in items_element.findall('item'):
                    item_dict = {child.tag: child.text.strip() if child.text else '' 
                                for child in item}
                    all_items.append(item_dict)
                
                return all_items
            
            elif result_code == '99':  # 데이터 없음
                return []
            
            else:  # 기타 오류
                msg_element = root.find('header/resultMsg')
                msg = msg_element.text if msg_element is not None else "메시지 없음"
                logger.warning(f"API 오류 - 지역코드: {lawd_cd}, 코드: {result_code}, 메시지: {msg}")
                return []
                
        except requests.exceptions.ConnectionError as e:
            wait_time = min(2 ** attempt * 5, 60)  # 지수 백오프 (최대 60초)
            logger.warning(f"연결 오류 - 지역코드: {lawd_cd} (시도 {attempt + 1}/{max_retries}), "
                         f"{wait_time}초 후 재시도...")
            time.sleep(wait_time)
            
        except requests.exceptions.Timeout:
            logger.warning(f"타임아웃 - 지역코드: {lawd_cd} (시도 {attempt + 1}/{max_retries})")
            time.sleep(10)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"요청 오류 - 지역코드: {lawd_cd}, 오류: {type(e).__name__}: {str(e)}")
            time.sleep(5)
            
        except ET.ParseError as e:
            logger.error(f"XML 파싱 오류 - 지역코드: {lawd_cd}, 오류: {e}")
            return []
    
    logger.error(f"최종 실패 - 지역코드: {lawd_cd}, 모든 재시도 실패")
    return []

def create_unique_id(df):
    """고유 ID 생성"""
    if df.empty:
        return df
    
    id_cols = ['거래금액', '년', '월', '일', '전용면적', '지번', '층', 
               '법정동시군구코드', '법정동읍면동코드']
    valid_cols = [col for col in id_cols if col in df.columns]
    
    if valid_cols:
        df['unique_id'] = df[valid_cols].astype(str).agg('_'.join, axis=1)
    else:
        logger.warning("고유 ID 생성을 위한 컬럼이 부족합니다.")
        df['unique_id'] = df.index.astype(str)
    
    return df

def find_and_upload_new_data(df_new, df_existing, worksheet):
    """새로운 데이터 찾기 및 업로드"""
    if df_new.empty:
        return 0, df_existing
    
    df_new = create_unique_id(df_new)
    
    if not df_existing.empty:
        if 'unique_id' not in df_existing.columns:
            df_existing = create_unique_id(df_existing)
        newly_added_df = df_new[~df_new['unique_id'].isin(df_existing['unique_id'])].copy()
    else:
        newly_added_df = df_new.copy()
    
    if newly_added_df.empty:
        return 0, df_existing
    
    added_count = len(newly_added_df)
    logger.info(f"총 {added_count}건의 신규 데이터를 확인했습니다. 시트에 추가합니다.")
    
    # unique_id 컬럼 제거
    df_to_upload = newly_added_df.drop(columns=['unique_id'], errors='ignore')
    
    try:
        if worksheet.row_count < 2:
            # 빈 시트인 경우
            set_with_dataframe(worksheet, df_to_upload, include_index=False, allow_formulas=False)
        else:
            # 기존 데이터가 있는 경우
            sheet_headers = [col.strip() for col in worksheet.row_values(1)]
            df_aligned = pd.DataFrame(columns=sheet_headers)
            
            for col in sheet_headers:
                if col in df_to_upload.columns:
                    df_aligned[col] = df_to_upload[col]
            
            worksheet.append_rows(df_aligned.values.tolist(), value_input_option='USER_ENTERED')
        
        # 업데이트된 데이터프레임 반환
        df_existing_updated = pd.concat([df_existing, newly_added_df], ignore_index=True)
        return added_count, df_existing_updated
        
    except Exception as e:
        logger.error(f"시트 쓰기 중 오류 발생: {e}")
        return -1, df_existing

def main():
    """메인 함수"""
    # 환경 변수 확인
    if not SERVICE_KEY or not GOOGLE_CREDENTIALS_JSON:
        logger.error("오류: SERVICE_KEY 또는 GOOGLE_CREDENTIALS_JSON Secret이 설정되지 않았습니다.")
        return
    
    logger.info(f"===== 전국 아파트 실거래가 업데이트 시작 (대상 월: {MONTHS_TO_FETCH}) =====")
    
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
            # 시트가 없으면 생성
            sh = gc.create(GOOGLE_SHEET_NAME)
            logger.info(f"'{GOOGLE_SHEET_NAME}' 시트를 새로 생성했습니다.")
            
            # 권한 설정 (선택사항)
            service_account_email = creds.get('client_email')
            if service_account_email:
                sh.share(service_account_email, perm_type='user', role='writer')
        
        worksheet = sh.get_worksheet(0)
        
        # 기존 데이터 읽기
        logger.info("구글 시트에서 기존 데이터를 읽어옵니다...")
        existing_records = worksheet.get_all_records()
        df_existing = pd.DataFrame(existing_records)
        
        if not df_existing.empty:
            df_existing.columns = df_existing.columns.str.strip()
            
    except Exception as e:
        logger.error(f"구글 시트 초기화 중 오류 발생: {e}")
        return
    
    # 세션 생성 (재시도 정책 포함)
    session = create_session_with_retry()
    total_added_count = 0
    
    # 각 월별로 데이터 수집
    for month in MONTHS_TO_FETCH:
        logger.info(f"\n--- {month} 데이터 처리 시작 ---")
        monthly_data = []
        failed_regions = []
        
        for i, code in enumerate(lawd_codes):
            print(f"\r  [{i+1}/{len(lawd_codes)}] {code} 수집 중...", end="", flush=True)
            
            region_data = fetch_data_for_region(session, code, month, SERVICE_KEY)
            
            if region_data:
                monthly_data.extend(region_data)
            else:
                failed_regions.append(code)
            
            # API 호출 간격 조정 (rate limiting 방지)
            time.sleep(0.5)  # 0.1초에서 0.5초로 증가
            
            # 10개 지역마다 추가 대기
            if (i + 1) % 10 == 0:
                time.sleep(2)
        
        print()  # 줄바꿈
        
        if failed_regions:
            logger.warning(f"{month}월 실패 지역 코드 ({len(failed_regions)}개): {failed_regions[:5]}...")
        
        if not monthly_data:
            logger.warning(f"{month}월에 수집된 데이터가 없습니다.")
            continue
        
        # 데이터프레임 생성
        df_month = pd.DataFrame(monthly_data)
        df_month.columns = df_month.columns.str.strip()
        logger.info(f"{month}월 데이터 총 {len(df_month)}건 수집 완료.")
        
        # 신규 데이터 업로드
        added_count, df_existing = find_and_upload_new_data(df_month, df_existing, worksheet)
        
        if added_count > 0:
            total_added_count += added_count
            logger.info(f"{month}월 데이터 중 {added_count}건 신규 추가 완료.")
        elif added_count == 0:
            logger.info(f"{month}월 데이터는 모두 중복입니다.")
        else:
            logger.error(f"{month}월 데이터 처리 중 오류가 발생했습니다.")
    
    logger.info(f"\n===== 전체 프로세스 완료! 총 {total_added_count}건의 신규 데이터 추가됨 =====")

if __name__ == '__main__':
    main()
