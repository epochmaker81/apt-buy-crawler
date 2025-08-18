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
import traceback
import urllib3

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 설정 변수 ---
SERVICE_KEY = os.getenv('SERVICE_KEY')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_NAME = '전국 아파트 매매 실거래가_누적'
LAWD_CODE_FILE = 'lawd_code.csv'
BASE_URL = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'

# 실행 모드 설정
RUN_MODE = os.getenv('RUN_MODE', 'TEST')

# 날짜 설정
today_kst = datetime.utcnow() + timedelta(hours=9)

if RUN_MODE == 'TEST':
    MONTHS_TO_FETCH = [today_kst.strftime('%Y%m')]
    TARGET_REGIONS = ['11110']  # 종로구 1개만
elif RUN_MODE == 'QUICK':
    MONTHS_TO_FETCH = [today_kst.strftime('%Y%m')]
    TARGET_REGIONS = None
else:
    MONTHS_TO_FETCH = []
    for i in range(3):
        target_date = today_kst - relativedelta(months=i)
        MONTHS_TO_FETCH.append(target_date.strftime('%Y%m'))
    TARGET_REGIONS = None

def get_google_creds():
    """Google 인증 정보 가져오기"""
    if GOOGLE_CREDENTIALS_JSON is None:
        logger.error("❌ GOOGLE_CREDENTIALS_JSON이 설정되지 않았습니다.")
        return None
    try:
        creds = json.loads(GOOGLE_CREDENTIALS_JSON)
        logger.info("✅ Google 인증 정보 로드 성공")
        return creds
    except json.JSONDecodeError as e:
        logger.error(f"❌ GOOGLE_CREDENTIALS_JSON 파싱 오류: {e}")
        return None

def get_lawd_codes(filepath):
    """지역 코드 파일 읽기"""
    try:
        if not os.path.exists(filepath):
            logger.error(f"❌ 파일이 존재하지 않습니다: {filepath}")
            return []
        
        df = pd.read_csv(filepath)
        logger.info(f"📁 CSV 파일 읽기 성공: {len(df)}개 지역")
        
        if 'code' not in df.columns:
            logger.error("❌ 'code' 컬럼이 없습니다.")
            return []
        
        codes = df['code'].astype(str).str.strip().tolist()
        
        # 유효한 코드만 필터링
        valid_codes = []
        for code in codes:
            if code and code != 'nan' and len(code) == 5 and code.isdigit():
                if not code.endswith('000'):
                    valid_codes.append(code)
        
        logger.info(f"✅ 유효한 지역 코드: {len(valid_codes)}개")
        
        # 필터링 적용
        if TARGET_REGIONS:
            codes = [c for c in valid_codes if c in TARGET_REGIONS]
            logger.info(f"🎯 TARGET_REGIONS 필터링 후: {len(codes)}개 - {codes}")
        elif RUN_MODE == 'QUICK':
            major_city_prefixes = ['11', '26', '27', '28', '29', '30', '31', '36']
            codes = [c for c in valid_codes if any(c.startswith(p) for p in major_city_prefixes)]
            logger.info(f"🏙️ 주요 도시 필터링 후: {len(codes)}개")
        else:
            codes = valid_codes
        
        if not codes:
            logger.error("❌ 처리할 유효한 지역 코드가 없습니다.")
            return []
        
        return codes
        
    except Exception as e:
        logger.error(f"❌ 지역 코드 파일 읽기 오류: {e}")
        return []

def create_session():
    """안전한 HTTP 세션 생성"""
    session = requests.Session()
    
    # HTTP 헤더 설정
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/xml, text/xml, */*',
        'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache'
    })
    
    # SSL 설정
    session.verify = False
    
    # 어댑터 설정 (재시도 로직)
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def test_network_connection():
    """네트워크 연결 테스트"""
    print("🌐 네트워크 연결 테스트 중...")
    
    test_urls = [
        'http://www.google.com',
        'http://openapi.molit.go.kr',
        'https://openapi.molit.go.kr'
    ]
    
    session = create_session()
    
    for url in test_urls:
        try:
            print(f"📡 {url} 테스트...")
            response = session.get(url, timeout=10)
            print(f"   ✅ {url}: {response.status_code}")
        except Exception as e:
            print(f"   ❌ {url}: {str(e)[:50]}")
    
    session.close()

def fetch_data_robust(lawd_cd, deal_ymd, service_key):
    """강화된 데이터 수집 (네트워크 문제 해결)"""
    print(f"🔄 [{lawd_cd}] 데이터 수집 시도...")
    
    params = {
        'serviceKey': service_key,
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ymd,
        'pageNo': '1',
        'numOfRows': '100'
    }
    
    # HTTP와 HTTPS 둘 다 시도
    urls_to_try = [
        'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade',
        'https://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'
    ]
    
    for url_idx, url in enumerate(urls_to_try):
        print(f"🌐 [{lawd_cd}] URL {url_idx+1}/2 시도: {'HTTP' if url_idx == 0 else 'HTTPS'}")
        
        # 세션별로 여러 번 시도
        for attempt in range(3):
            session = None
            try:
                print(f"   📡 [{lawd_cd}] 시도 {attempt+1}/3...")
                
                session = create_session()
                
                response = session.get(
                    url,
                    params=params,
                    timeout=30,
                    verify=False
                )
                
                print(f"   📡 [{lawd_cd}] HTTP 상태: {response.status_code}")
                
                if response.status_code == 200:
                    print(f"   📄 [{lawd_cd}] 응답 수신 성공 ({len(response.content)} bytes)")
                    
                    try:
                        root = ET.fromstring(response.content)
                        result_code = root.find('.//resultCode')
                        result_msg = root.find('.//resultMsg')
                        
                        if result_code is not None:
                            code = result_code.text
                            msg = result_msg.text if result_msg is not None else "메시지 없음"
                            
                            print(f"   🎯 [{lawd_cd}] API 결과: {code} - {msg}")
                            
                            if code == '00':
                                # 성공
                                items_element = root.find('.//items')
                                if items_element:
                                    items = items_element.findall('item')
                                    print(f"   ✅ [{lawd_cd}] 성공! {len(items)}건 수집")
                                    
                                    items_data = []
                                    for item in items:
                                        item_dict = {}
                                        for child in item:
                                            item_dict[child.tag] = child.text.strip() if child.text else ''
                                        items_data.append(item_dict)
                                    
                                    if items_data:
                                        print(f"   📊 [{lawd_cd}] 첫 번째 데이터 컬럼: {list(items_data[0].keys())[:5]}...")
                                    
                                    session.close()
                                    return items_data
                                else:
                                    print(f"   📭 [{lawd_cd}] items 엘리먼트 없음")
                                    session.close()
                                    return []
                            elif code == '99':
                                print(f"   📭 [{lawd_cd}] 데이터 없음 (정상)")
                                session.close()
                                return []
                            elif code == '04':
                                print(f"   ❌ [{lawd_cd}] SERVICE_KEY 오류")
                                session.close()
                                return []
                            elif code == '05':
                                print(f"   ❌ [{lawd_cd}] 접근 권한 없음")
                                session.close()
                                return []
                            else:
                                print(f"   ❌ [{lawd_cd}] API 오류: {code}")
                                session.close()
                                return []
                        else:
                            print(f"   ❌ [{lawd_cd}] 결과 코드 없음")
                            
                    except ET.ParseError as e:
                        print(f"   ❌ [{lawd_cd}] XML 파싱 오류: {str(e)[:30]}")
                        print(f"   📄 응답 내용 (첫 200자): {response.text[:200]}")
                        
                else:
                    print(f"   ❌ [{lawd_cd}] HTTP 오류: {response.status_code}")
                    
            except requests.exceptions.ConnectTimeout:
                print(f"   ⏰ [{lawd_cd}] 연결 타임아웃")
            except requests.exceptions.ReadTimeout:
                print(f"   ⏰ [{lawd_cd}] 읽기 타임아웃")
            except requests.exceptions.ConnectionError as e:
                print(f"   🌐 [{lawd_cd}] 연결 오류: {str(e)[:50]}")
            except Exception as e:
                print(f"   ❌ [{lawd_cd}] 예외: {str(e)[:50]}")
            finally:
                if session:
                    session.close()
            
            # 재시도 전 대기
            if attempt < 2:
                wait_time = (attempt + 1) * 2
                print(f"   ⏳ [{lawd_cd}] {wait_time}초 대기 후 재시도...")
                time.sleep(wait_time)
    
    print(f"   ❌ [{lawd_cd}] 모든 시도 실패")
    return []

def create_unique_id(df):
    """고유 ID 생성"""
    if df.empty:
        return df
    
    id_cols = ['거래금액', '년', '월', '일', '전용면적', '지번', '층']
    valid_cols = [col for col in id_cols if col in df.columns]
    
    if valid_cols:
        df['unique_id'] = df[valid_cols].astype(str).agg('_'.join, axis=1)
    return df

def upload_to_sheet(df_new, df_existing, worksheet):
    """Google Sheets에 업로드"""
    if df_new.empty:
        print("📭 업로드할 데이터가 없습니다.")
        return 0, df_existing
    
    print(f"📊 새 데이터: {len(df_new)}건")
    print(f"📋 컬럼: {list(df_new.columns)}")
    
    df_new = create_unique_id(df_new)
    
    if not df_existing.empty:
        if 'unique_id' not in df_existing.columns:
            df_existing = create_unique_id(df_existing)
        
        newly_added = df_new[~df_new['unique_id'].isin(df_existing['unique_id'])].copy()
        print(f"🔍 중복 제거 후: {len(newly_added)}건")
    else:
        newly_added = df_new.copy()
        print("📝 기존 데이터 없음 - 모든 데이터 추가")
    
    if newly_added.empty:
        print("ℹ️ 추가할 새 데이터 없음 (모두 중복)")
        return 0, df_existing
    
    count = len(newly_added)
    df_to_upload = newly_added.drop(columns=['unique_id'], errors='ignore')
    
    try:
        if worksheet.row_count < 2:
            print("📝 빈 시트에 데이터 추가")
            set_with_dataframe(worksheet, df_to_upload, include_index=False)
        else:
            print("📝 기존 시트에 데이터 추가")
            headers = worksheet.row_values(1)
            
            aligned = pd.DataFrame(columns=headers)
            for col in headers:
                if col in df_to_upload.columns:
                    aligned[col] = df_to_upload[col]
                else:
                    aligned[col] = ''
            
            worksheet.append_rows(aligned.values.tolist(), value_input_option='USER_ENTERED')
        
        df_existing_updated = pd.concat([df_existing, newly_added], ignore_index=True)
        print(f"✅ 업로드 성공: {count}건 추가됨")
        return count, df_existing_updated
        
    except Exception as e:
        print(f"❌ 업로드 오류: {e}")
        logger.error(f"업로드 오류: {e}")
        return -1, df_existing

def main():
    """메인 함수"""
    start_time = time.time()
    
    print("🚀 ===== 아파트 실거래가 업데이트 시작 =====")
    print(f"🔧 실행 모드: {RUN_MODE}")
    print(f"📅 대상 월: {MONTHS_TO_FETCH}")
    
    # 환경 변수 확인
    if not SERVICE_KEY:
        print("❌ SERVICE_KEY가 설정되지 않았습니다.")
        return
    
    if not GOOGLE_CREDENTIALS_JSON:
        print("❌ GOOGLE_CREDENTIALS_JSON이 설정되지 않았습니다.")
        return
    
    print(f"✅ SERVICE_KEY: {len(SERVICE_KEY)}자")
    print(f"✅ GOOGLE_CREDENTIALS_JSON: 설정됨")
    
    # 네트워크 연결 테스트
    test_network_connection()
    
    # 지역 코드 로드
    lawd_codes = get_lawd_codes(LAWD_CODE_FILE)
    if not lawd_codes:
        print("❌ 유효한 지역 코드가 없습니다.")
        return
    
    print(f"✅ 지역 코드: {len(lawd_codes)}개 - {lawd_codes}")
    
    # Google Sheets 초기화
    print("📊 Google Sheets 초기화 중...")
    try:
        creds = get_google_creds()
        if not creds:
            return
        
        gc = gspread.service_account_from_dict(creds)
        print("✅ Google Sheets 클라이언트 생성")
        
        try:
            sh = gc.open(GOOGLE_SHEET_NAME)
            print(f"✅ 기존 시트 열기: {GOOGLE_SHEET_NAME}")
        except gspread.exceptions.SpreadsheetNotFound:
            sh = gc.create(GOOGLE_SHEET_NAME)
            print(f"🆕 새 시트 생성: {GOOGLE_SHEET_NAME}")
        
        worksheet = sh.get_worksheet(0)
        sheet_url = sh.url
        print(f"🔗 시트 URL: {sheet_url}")
        
        # 기존 데이터 읽기
        existing_records = worksheet.get_all_records()
        df_existing = pd.DataFrame(existing_records)
        
        if not df_existing.empty:
            df_existing.columns = df_existing.columns.str.strip()
            print(f"📊 기존 데이터: {len(df_existing)}건")
        else:
            print("📝 기존 데이터 없음")
            
    except Exception as e:
        print(f"❌ Google Sheets 오류: {e}")
        logger.error(traceback.format_exc())
        return
    
    total_added = 0
    
    # 데이터 수집 및 업로드
    for month in MONTHS_TO_FETCH:
        print(f"\n📅 ===== {month} 데이터 수집 =====")
        
        monthly_data = []
        
        for i, code in enumerate(lawd_codes):
            print(f"\n[{i+1}/{len(lawd_codes)}] {code} 처리...")
            data = fetch_data_robust(code, month, SERVICE_KEY)
            
            if data:
                monthly_data.extend(data)
                print(f"   ✅ {len(data)}건 수집됨")
            else:
                print(f"   📭 데이터 없음")
            
            time.sleep(2)  # API 호출 간격 증가
        
        print(f"\n📊 {month} 총 수집: {len(monthly_data)}건")
        
        if not monthly_data:
            print(f"⚠️ {month}: 수집된 데이터 없음")
            print("가능한 원인:")
            print("1. 네트워크 연결 문제")
            print("2. SERVICE_KEY 권한 문제")
            print("3. API 서버 일시적 문제")
            continue
        
        # 데이터프레임 생성
        df_month = pd.DataFrame(monthly_data)
        df_month.columns = df_month.columns.str.strip()
        
        print(f"📤 Google Sheets에 업로드 중...")
        added, df_existing = upload_to_sheet(df_month, df_existing, worksheet)
        
        if added > 0:
            total_added += added
            print(f"✅ {month}: {added}건 추가됨")
        elif added == 0:
            print(f"ℹ️ {month}: 새로운 데이터 없음")
        else:
            print(f"❌ {month}: 업로드 실패")
    
    # 완료
    elapsed = time.time() - start_time
    print(f"\n🎉 ===== 완료 =====")
    print(f"📊 총 {total_added}건 추가")
    print(f"⏱️ 소요 시간: {elapsed//60:.0f}분 {elapsed%60:.0f}초")
    print(f"🔗 결과: {sheet_url}")

if __name__ == '__main__':
    main()
