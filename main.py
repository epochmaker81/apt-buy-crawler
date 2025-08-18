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
import random

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 설정 변수 ---
SERVICE_KEY = os.getenv('SERVICE_KEY')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_NAME = '전국 아파트 매매 실거래가_누적'
LAWD_CODE_FILE = 'lawd_code.csv'

# 대안 API URL들
API_URLS = [
    'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade',
    'https://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade',
    'http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade'  # 대안 URL
]

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

def test_basic_network():
    """기본 네트워크 연결 테스트"""
    print("🌐 기본 네트워크 테스트...")
    
    test_sites = [
        'http://www.google.com',
        'http://httpbin.org/get',
        'https://api.github.com',
        'http://data.go.kr'
    ]
    
    for site in test_sites:
        try:
            response = requests.get(site, timeout=10)
            print(f"   ✅ {site}: {response.status_code}")
        except Exception as e:
            print(f"   ❌ {site}: {str(e)[:50]}")

def generate_sample_data(lawd_cd, deal_ymd):
    """샘플 데이터 생성 (API 접근 불가 시 대안)"""
    print(f"🎭 [{lawd_cd}] 샘플 데이터 생성 중...")
    
    # 실제와 유사한 샘플 데이터
    sample_data = []
    
    for i in range(random.randint(0, 5)):  # 0-5건 랜덤 생성
        data = {
            '거래금액': f"{random.randint(30000, 150000)}",  # 3억~15억
            '건축년도': str(random.randint(1990, 2020)),
            '년': deal_ymd[:4],
            '월': deal_ymd[4:6],
            '일': f"{random.randint(1, 28):02d}",
            '전용면적': f"{random.randint(60, 150)}.{random.randint(10, 99)}",
            '지번': f"{random.randint(1, 999)}",
            '층': str(random.randint(1, 20)),
            '법정동': f"테스트동{random.randint(1, 5)}가",
            '아파트': f"테스트아파트{random.randint(1, 10)}",
            '법정동시군구코드': lawd_cd,
            '법정동읍면동코드': f"{random.randint(10100, 10999)}",
            '도로명': f"테스트로{random.randint(1, 100)}",
            '해제사유발생일': '',
            '거래유형': '직거래',
            '중개사소재지': '',
            '해제여부': 'O'
        }
        sample_data.append(data)
    
    if sample_data:
        print(f"   🎭 [{lawd_cd}] 샘플 {len(sample_data)}건 생성됨")
    else:
        print(f"   📭 [{lawd_cd}] 샘플 데이터 없음")
    
    return sample_data

def try_api_with_fallback(lawd_cd, deal_ymd, service_key):
    """API 시도 후 실패 시 샘플 데이터 생성"""
    print(f"🔄 [{lawd_cd}] 실제 API 시도 중...")
    
    params = {
        'serviceKey': service_key,
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ymd,
        'pageNo': '1',
        'numOfRows': '100'
    }
    
    # 모든 API URL 시도
    for url_idx, base_url in enumerate(API_URLS):
        print(f"   🌐 [{lawd_cd}] API URL {url_idx+1}/{len(API_URLS)} 시도...")
        
        for attempt in range(2):  # 각 URL당 2번씩만 시도
            try:
                print(f"      📡 [{lawd_cd}] 시도 {attempt+1}/2...")
                
                # requests 설정
                session = requests.Session()
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (compatible; DataCollector/1.0)',
                    'Accept': 'application/xml, text/xml, */*'
                })
                
                response = session.get(
                    base_url,
                    params=params,
                    timeout=15,  # 타임아웃 단축
                    verify=False
                )
                
                print(f"      📡 [{lawd_cd}] HTTP: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        root = ET.fromstring(response.content)
                        result_code = root.find('.//resultCode')
                        
                        if result_code is not None:
                            code = result_code.text
                            print(f"      🎯 [{lawd_cd}] 결과: {code}")
                            
                            if code == '00':
                                items_element = root.find('.//items')
                                if items_element:
                                    items = items_element.findall('item')
                                    if items:
                                        print(f"      ✅ [{lawd_cd}] 실제 API 성공! {len(items)}건")
                                        
                                        items_data = []
                                        for item in items:
                                            item_dict = {}
                                            for child in item:
                                                item_dict[child.tag] = child.text.strip() if child.text else ''
                                            items_data.append(item_dict)
                                        
                                        session.close()
                                        return items_data, True  # True = 실제 데이터
                                    else:
                                        print(f"      📭 [{lawd_cd}] 데이터 없음")
                                        session.close()
                                        return [], True
                            elif code == '99':
                                print(f"      📭 [{lawd_cd}] 데이터 없음 (정상)")
                                session.close()
                                return [], True
                            else:
                                print(f"      ❌ [{lawd_cd}] API 오류: {code}")
                        
                    except ET.ParseError:
                        print(f"      ❌ [{lawd_cd}] XML 파싱 오류")
                
                session.close()
                
            except Exception as e:
                print(f"      ❌ [{lawd_cd}] 연결 오류: {str(e)[:30]}")
            
            if attempt < 1:
                time.sleep(1)
    
    # 모든 API 시도 실패 → 샘플 데이터 생성
    print(f"   🎭 [{lawd_cd}] API 실패 → 샘플 데이터 사용")
    sample_data = generate_sample_data(lawd_cd, deal_ymd)
    return sample_data, False  # False = 샘플 데이터

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

def create_unique_id(df):
    """고유 ID 생성"""
    if df.empty:
        return df
    
    id_cols = ['거래금액', '년', '월', '일', '전용면적', '지번', '층']
    valid_cols = [col for col in id_cols if col in df.columns]
    
    if valid_cols:
        df['unique_id'] = df[valid_cols].astype(str).agg('_'.join, axis=1)
    return df

def upload_to_sheet(df_new, df_existing, worksheet, is_real_data=True):
    """Google Sheets에 업로드"""
    if df_new.empty:
        print("📭 업로드할 데이터가 없습니다.")
        return 0, df_existing
    
    data_type = "실제 데이터" if is_real_data else "샘플 데이터"
    print(f"📊 새 {data_type}: {len(df_new)}건")
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
    
    # 데이터 타입 표시 컬럼 추가
    newly_added['데이터_타입'] = data_type
    newly_added['수집_시간'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    count = len(newly_added)
    df_to_upload = newly_added.drop(columns=['unique_id'], errors='ignore')
    
    try:
        if worksheet.row_count < 2:
            print("📝 빈 시트에 데이터 추가")
            set_with_dataframe(worksheet, df_to_upload, include_index=False)
        else:
            print("📝 기존 시트에 데이터 추가")
            
            # 새 컬럼이 있으면 헤더 업데이트
            existing_headers = worksheet.row_values(1)
            new_headers = list(df_to_upload.columns)
            
            if set(new_headers) != set(existing_headers):
                print("📋 헤더 업데이트 필요")
                # 기존 데이터 읽기
                all_data = worksheet.get_all_records()
                existing_df = pd.DataFrame(all_data)
                
                # 새 컬럼 추가
                for col in new_headers:
                    if col not in existing_df.columns:
                        existing_df[col] = ''
                
                # 전체 재업로드
                combined_df = pd.concat([existing_df, df_to_upload], ignore_index=True)
                worksheet.clear()
                set_with_dataframe(worksheet, combined_df, include_index=False)
            else:
                # 기존 방식으로 추가
                worksheet.append_rows(df_to_upload.values.tolist(), value_input_option='USER_ENTERED')
        
        df_existing_updated = pd.concat([df_existing, newly_added], ignore_index=True)
        print(f"✅ 업로드 성공: {count}건 {data_type} 추가됨")
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
    
    # 네트워크 테스트
    test_basic_network()
    
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
    real_data_count = 0
    sample_data_count = 0
    
    # 데이터 수집 및 업로드
    for month in MONTHS_TO_FETCH:
        print(f"\n📅 ===== {month} 데이터 수집 =====")
        
        monthly_data = []
        monthly_real_data = True
        
        for i, code in enumerate(lawd_codes):
            print(f"\n[{i+1}/{len(lawd_codes)}] {code} 처리...")
            data, is_real = try_api_with_fallback(code, month, SERVICE_KEY)
            
            if data:
                monthly_data.extend(data)
                if is_real:
                    real_data_count += len(data)
                    print(f"   ✅ 실제 데이터 {len(data)}건 수집")
                else:
                    sample_data_count += len(data)
                    print(f"   🎭 샘플 데이터 {len(data)}건 생성")
                    monthly_real_data = False
            else:
                print(f"   📭 데이터 없음")
            
            time.sleep(1)
        
        print(f"\n📊 {month} 총 수집: {len(monthly_data)}건")
        
        if not monthly_data:
            print(f"⚠️ {month}: 수집된 데이터 없음")
            continue
        
        # 데이터프레임 생성
        df_month = pd.DataFrame(monthly_data)
        df_month.columns = df_month.columns.str.strip()
        
        print(f"📤 Google Sheets에 업로드 중...")
        added, df_existing = upload_to_sheet(df_month, df_existing, worksheet, monthly_real_data)
        
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
    print(f"🔗 실제 데이터: {real_data_count}건")
    print(f"🎭 샘플 데이터: {sample_data_count}건")
    print(f"⏱️ 소요 시간: {elapsed//60:.0f}분 {elapsed%60:.0f}초")
    print(f"🔗 결과: {sheet_url}")
    
    if real_data_count > 0:
        print(f"\n🎉 성공! 실제 API에서 {real_data_count}건 수집됨")
    elif sample_data_count > 0:
        print(f"\n⚠️ API 접근 불가로 샘플 데이터 {sample_data_count}건 생성됨")
        print("📝 참고: 샘플 데이터는 시스템 테스트용입니다.")
    else:
        print(f"\n📭 수집된 데이터가 없습니다.")

if __name__ == '__main__':
    main()
