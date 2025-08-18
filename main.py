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

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 설정 변수 ---
SERVICE_KEY = os.getenv('SERVICE_KEY')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_NAME = '전국 아파트 매매 실거래가_누적'
LAWD_CODE_FILE = 'lawd_code.csv'
BASE_URL = 'https://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'

# 실행 모드 설정
RUN_MODE = os.getenv('RUN_MODE', 'TEST')

# 날짜 설정
today_kst = datetime.utcnow() + timedelta(hours=9)

if RUN_MODE == 'TEST':
    MONTHS_TO_FETCH = [today_kst.strftime('%Y%m')]
    TARGET_REGIONS = ['11110', '11140', '11170']  # 서울 3개 구
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
        logger.info(f"✅ Google 인증 정보 로드 성공 - 프로젝트: {creds.get('project_id', 'Unknown')}")
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
                if not code.endswith('000'):  # 광역시도 코드 제외
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

def test_api_connection(lawd_cd, deal_ymd, service_key):
    """API 연결 테스트"""
    logger.info(f"🧪 API 연결 테스트: {lawd_cd} - {deal_ymd}")
    
    params = {
        'serviceKey': service_key,
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ymd,
        'pageNo': '1',
        'numOfRows': '5'
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        logger.info(f"📡 HTTP 상태: {response.status_code}")
        
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            result_code = root.find('.//resultCode')
            result_msg = root.find('.//resultMsg')
            
            if result_code is not None:
                code = result_code.text
                msg = result_msg.text if result_msg is not None else "메시지 없음"
                logger.info(f"🎯 API 응답: {code} - {msg}")
                
                if code == '00':
                    items = root.findall('.//item')
                    logger.info(f"✅ API 테스트 성공! 아이템 수: {len(items)}")
                    return True
                elif code == '99':
                    logger.info(f"⚠️ 데이터 없음 (정상) - {lawd_cd}")
                    return True
                elif code == '04':
                    logger.error(f"❌ SERVICE_KEY 오류")
                    return False
                elif code == '05':
                    logger.error(f"❌ 서비스 접근 권한 없음")
                    return False
                else:
                    logger.error(f"❌ API 오류: {code} - {msg}")
                    return False
            else:
                logger.error("❌ 결과 코드를 찾을 수 없음")
                return False
        else:
            logger.error(f"❌ HTTP 오류: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"❌ API 테스트 예외: {e}")
        return False

def fetch_data_simple(lawd_cd, deal_ymd, service_key):
    """데이터 수집"""
    params = {
        'serviceKey': service_key,
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ymd,
        'pageNo': '1',
        'numOfRows': '1000'
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            result_code = root.find('.//resultCode')
            
            if result_code is not None and result_code.text == '00':
                items_element = root.find('.//items')
                if items_element:
                    items_data = []
                    for item in items_element.findall('item'):
                        item_dict = {}
                        for child in item:
                            item_dict[child.tag] = child.text.strip() if child.text else ''
                        items_data.append(item_dict)
                    return items_data
                else:
                    return []
            elif result_code is not None and result_code.text == '99':
                return []  # 데이터 없음
            else:
                logger.warning(f"⚠️ {lawd_cd} API 응답: {result_code.text if result_code is not None else 'Unknown'}")
                return []
        else:
            logger.warning(f"⚠️ {lawd_cd} HTTP 오류: {response.status_code}")
            return []
            
    except Exception as e:
        logger.error(f"❌ {lawd_cd} 데이터 수집 오류: {str(e)[:50]}")
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
        logger.info("📭 업로드할 새 데이터가 없습니다.")
        return 0, df_existing
    
    logger.info(f"📊 새 데이터: {len(df_new)}건")
    logger.info(f"📋 새 데이터 컬럼: {list(df_new.columns)}")
    
    # 고유 ID 생성
    df_new = create_unique_id(df_new)
    
    if not df_existing.empty:
        if 'unique_id' not in df_existing.columns:
            df_existing = create_unique_id(df_existing)
        
        # 중복 제거
        newly_added = df_new[~df_new['unique_id'].isin(df_existing['unique_id'])].copy()
        logger.info(f"🔍 중복 제거 후: {len(newly_added)}건")
    else:
        newly_added = df_new.copy()
        logger.info("📝 기존 데이터 없음 - 모든 데이터 추가")
    
    if newly_added.empty:
        logger.info("ℹ️ 추가할 새 데이터 없음 (모두 중복)")
        return 0, df_existing
    
    count = len(newly_added)
    df_to_upload = newly_added.drop(columns=['unique_id'], errors='ignore')
    
    try:
        if worksheet.row_count < 2:
            logger.info("📝 빈 시트에 데이터 추가")
            set_with_dataframe(worksheet, df_to_upload, include_index=False)
        else:
            logger.info("📝 기존 시트에 데이터 추가")
            headers = worksheet.row_values(1)
            
            # 헤더에 맞춰 데이터 정렬
            aligned = pd.DataFrame(columns=headers)
            for col in headers:
                if col in df_to_upload.columns:
                    aligned[col] = df_to_upload[col]
                else:
                    aligned[col] = ''
            
            worksheet.append_rows(aligned.values.tolist(), value_input_option='USER_ENTERED')
        
        df_existing_updated = pd.concat([df_existing, newly_added], ignore_index=True)
        logger.info(f"✅ 업로드 성공: {count}건 추가됨")
        return count, df_existing_updated
        
    except Exception as e:
        logger.error(f"❌ 업로드 오류: {e}")
        logger.error(traceback.format_exc())
        return -1, df_existing

def main():
    """메인 함수"""
    start_time = time.time()
    
    print("🚀 ===== 아파트 실거래가 업데이트 시작 =====")
    logger.info("🚀 아파트 실거래가 업데이트 시작")
    logger.info(f"🔧 실행 모드: {RUN_MODE}")
    logger.info(f"📅 대상 월: {MONTHS_TO_FETCH}")
    
    # 환경 변수 확인
    if not SERVICE_KEY:
        logger.error("❌ SERVICE_KEY가 설정되지 않았습니다.")
        print("❌ SERVICE_KEY가 설정되지 않았습니다.")
        return
    
    if not GOOGLE_CREDENTIALS_JSON:
        logger.error("❌ GOOGLE_CREDENTIALS_JSON이 설정되지 않았습니다.")
        print("❌ GOOGLE_CREDENTIALS_JSON이 설정되지 않았습니다.")
        return
    
    logger.info(f"🔑 SERVICE_KEY 길이: {len(SERVICE_KEY)}")
    print(f"✅ 환경 변수 확인 완료 - SERVICE_KEY: {len(SERVICE_KEY)}자")
    
    # 지역 코드 로드
    lawd_codes = get_lawd_codes(LAWD_CODE_FILE)
    if not lawd_codes:
        logger.error("❌ 유효한 지역 코드가 없습니다.")
        print("❌ 유효한 지역 코드가 없습니다.")
        return
    
    print(f"✅ 지역 코드 로드 완료: {len(lawd_codes)}개")
    
    # API 연결 테스트
    print("🧪 API 연결 테스트 중...")
    if not test_api_connection(lawd_codes[0], MONTHS_TO_FETCH[0], SERVICE_KEY):
        logger.error("❌ API 연결 테스트 실패")
        print("❌ API 연결 테스트 실패 - SERVICE_KEY를 확인하세요.")
        return
    
    print("✅ API 연결 테스트 성공")
    
    # Google Sheets 초기화
    print("📊 Google Sheets 초기화 중...")
    try:
        creds = get_google_creds()
        if not creds:
            return
        
        gc = gspread.service_account_from_dict(creds)
        
        try:
            sh = gc.open(GOOGLE_SHEET_NAME)
            logger.info(f"✅ 기존 시트 열기: {GOOGLE_SHEET_NAME}")
        except gspread.exceptions.SpreadsheetNotFound:
            sh = gc.create(GOOGLE_SHEET_NAME)
            logger.info(f"🆕 새 시트 생성: {GOOGLE_SHEET_NAME}")
        
        worksheet = sh.get_worksheet(0)
        
        # 시트 URL 출력
        sheet_url = sh.url
        logger.info(f"🔗 시트 URL: {sheet_url}")
        print(f"🔗 구글 시트: {sheet_url}")
        
        # 기존 데이터 읽기
        existing_records = worksheet.get_all_records()
        df_existing = pd.DataFrame(existing_records)
        
        if not df_existing.empty:
            df_existing.columns = df_existing.columns.str.strip()
            logger.info(f"📊 기존 데이터: {len(df_existing)}건")
            print(f"📊 기존 데이터: {len(df_existing)}건")
        else:
            logger.info("📝 기존 데이터 없음")
            print("📝 기존 데이터 없음 - 새로 시작")
            
    except Exception as e:
        logger.error(f"❌ Google Sheets 초기화 오류: {e}")
        logger.error(traceback.format_exc())
        print(f"❌ Google Sheets 오류: {e}")
        return
    
    total_added = 0
    
    # 데이터 수집 및 업로드
    for month in MONTHS_TO_FETCH:
        print(f"\n📅 {month} 데이터 수집 시작...")
        logger.info(f"📅 {month} 처리 시작")
        
        monthly_data = []
        
        for i, code in enumerate(lawd_codes):
            print(f"🔄 [{i+1}/{len(lawd_codes)}] {code} 처리 중...")
            data = fetch_data_simple(code, month, SERVICE_KEY)
            
            if data:
                monthly_data.extend(data)
                logger.info(f"✅ {code}: {len(data)}건 수집")
                print(f"   ✅ {code}: {len(data)}건 수집")
            else:
                logger.info(f"📭 {code}: 데이터 없음")
                print(f"   📭 {code}: 데이터 없음")
            
            time.sleep(0.5)  # API 호출 간격
        
        print(f"📊 {month} 총 수집: {len(monthly_data)}건")
        logger.info(f"📊 {month} 총 수집: {len(monthly_data)}건")
        
        if not monthly_data:
            print(f"⚠️ {month}: 수집된 데이터 없음")
            continue
        
        # 데이터프레임 생성 및 업로드
        df_month = pd.DataFrame(monthly_data)
        df_month.columns = df_month.columns.str.strip()
        
        print(f"📤 Google Sheets에 업로드 중...")
        added, df_existing = upload_to_sheet(df_month, df_existing, worksheet)
        
        if added > 0:
            total_added += added
            print(f"✅ {month}: {added}건 추가됨")
            logger.info(f"✅ {month}: {added}건 추가")
        elif added == 0:
            print(f"ℹ️ {month}: 새로운 데이터 없음 (중복)")
            logger.info(f"ℹ️ {month}: 중복으로 추가 없음")
        else:
            print(f"❌ {month}: 업로드 실패")
            logger.error(f"❌ {month}: 업로드 실패")
    
    # 완료
    elapsed = time.time() - start_time
    print(f"\n🎉 ===== 완료 =====")
    print(f"📊 총 {total_added}건 추가")
    print(f"⏱️ 소요 시간: {elapsed//60:.0f}분 {elapsed%60:.0f}초")
    print(f"🔗 결과 확인: {sheet_url}")
    
    logger.info(f"🎉 완료 - 총 {total_added}건 추가, {elapsed//60:.0f}분 {elapsed%60:.0f}초")
    
    if total_added == 0:
        print("\n⚠️ 새로 추가된 데이터가 없습니다.")
        print("가능한 원인:")
        print("1. 해당 월에 실거래 데이터가 없음")
        print("2. 이미 모든 데이터가 시트에 존재")
        logger.warning("새로 추가된 데이터 없음")
    else:
        print(f"\n🎉 성공! Google Sheets에서 결과를 확인하세요!")

if __name__ == '__main__':
    main()
