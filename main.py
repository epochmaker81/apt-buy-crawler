# <<< 최종 버전 main.py (메모리 문제 해결) >>>

import os
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import xml.etree.ElementTree as ET
import time
import ssl
from requests.adapters import HTTPAdapter
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class CustomHttpAdapter(HTTPAdapter):
    # ... (이전과 동일)
    def __init__(self, *args, **kwargs):
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')
        super().__init__(*args, **kwargs)
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = requests.urllib3.PoolManager(
            num_pools=connections, maxsize=maxsize, block=block, ssl_context=self.ssl_context
        )

# --- 설정 변수 (이전과 동일) ---
SERVICE_KEY = os.getenv('SERVICE_KEY')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_NAME = '전국 아파트 매매 실거래가_누적'
LAWD_CODE_FILE = 'lawd_code.csv'
BASE_URL = 'https://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'

# --- 수집할 연월 동적 생성 (이전과 동일) ---
today_kst = datetime.utcnow() + timedelta(hours=9)
MONTHS_TO_FETCH = []
for i in range(3):
    target_date = today_kst - relativedelta(months=i)
    MONTHS_TO_FETCH.append(target_date.strftime('%Y%m'))

# --- 함수 정의 (이전과 동일, 일부 수정) ---
def get_google_creds():
    # ... (이전과 동일)
    if GOOGLE_CREDENTIALS_JSON is None: return None
    try: return json.loads(GOOGLE_CREDENTIALS_JSON)
    except json.JSONDecodeError: return None

def get_lawd_codes(filepath):
    # ... (이전과 동일)
    try:
        df = pd.read_csv(filepath)
        print(f"총 {len(df['code'])}개의 지역 코드를 불러왔습니다.")
        return df['code'].astype(str).tolist()
    except FileNotFoundError:
        print(f"오류: {filepath} 파일을 찾을 수 없습니다.")
        return []

def fetch_data_for_region(session, lawd_cd, deal_ymd, service_key):
    # ... (이전과 동일)
    params = {'serviceKey': service_key, 'LAWD_CD': lawd_cd, 'DEAL_YMD': deal_ymd, 'pageNo': '1', 'numOfRows': '5000'}
    for attempt in range(3):
        try:
            response = session.get(BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            all_items = []
            root = ET.fromstring(response.content)
            result_code_element = root.find('header/resultCode')
            if result_code_element is None or result_code_element.text != '00':
                if result_code_element is None or result_code_element.text != '99':
                    print(f"\n  [API 응답 오류] 지역코드: {lawd_cd}")
                return []
            items_element = root.find('body/items')
            if items_element is None: return []
            for item in items_element.findall('item'):
                item_dict = {child.tag: child.text.strip() if child.text else '' for child in item}
                all_items.append(item_dict)
            return all_items
        except requests.exceptions.RequestException as e:
            print(f"\n  [네트워크 오류] 지역코드: {lawd_cd} (시도 {attempt + 1}/3). 오류 유형: {type(e).__name__}")
            time.sleep(3)
        except ET.ParseError as e:
            print(f"\n  [XML 파싱 오류] 지역코드: {lawd_cd}, 오류: {e}")
            return []
    print(f"\n  [네트워크 오류] 지역코드: {lawd_cd}, 최종 접속 실패.")
    return []

def create_unique_id(df):
    # ... (이전과 동일)
    if df.empty: return df
    id_cols = ['거래금액', '년', '월', '일', '전용면적', '지번', '층', '법정동시군구코드', '법정동읍면동코드']
    valid_cols = [col for col in id_cols if col in df.columns]
    df['unique_id'] = df[valid_cols].astype(str).agg('_'.join, axis=1)
    return df

# [수정] 구글 시트 업데이트 로직을 별도 함수로 분리
def update_google_sheet(df_new, gc):
    """새로운 데이터프레임을 받아 기존 구글 시트와 비교하고 업데이트합니다."""
    if df_new.empty:
        print("업데이트할 신규 데이터가 없습니다.")
        return 0

    try:
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.get_worksheet(0)
        print("구글 시트에서 기존 데이터를 읽어옵니다...")
        existing_records = worksheet.get_all_records()
        df_existing = pd.DataFrame(existing_records)
        if not df_existing.empty:
            df_existing.columns = df_existing.columns.str.strip()
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"경고: '{GOOGLE_SHEET_NAME}' 시트를 찾을 수 없어 새로 생성합니다.")
        df_existing = pd.DataFrame()
        worksheet = None
    except Exception as e:
        print(f"구글 시트 처리 중 오류 발생: {e}")
        return -1 # 오류 발생을 알리기 위해 음수 반환

    df_new = create_unique_id(df_new)
    if not df_existing.empty:
        df_existing = create_unique_id(df_existing)
        newly_added_df = df_new[~df_new['unique_id'].isin(df_existing['unique_id'])].copy()
    else:
        newly_added_df = df_new.copy()

    if newly_added_df.empty:
        print("추가할 새로운 거래 데이터가 없습니다.")
        return 0
    
    added_count = len(newly_added_df)
    print(f"총 {added_count}건의 신규 데이터를 확인했습니다. 시트에 추가합니다.")
    if 'unique_id' in newly_added_df.columns:
        newly_added_df.drop(columns=['unique_id'], inplace=True)

    try:
        if worksheet is None:
            sh = gc.create(GOOGLE_SHEET_NAME)
            worksheet = sh.get_worksheet(0)
            service_account_email = os.getenv('GSPREAD_SERVICE_ACCOUNT_EMAIL')
            if service_account_email: sh.share(service_account_email, perm_type='user', role='writer')
            set_with_dataframe(worksheet, newly_added_df, include_index=False, allow_formulas=False)
        elif df_existing.empty:
            set_with_dataframe(worksheet, newly_added_df, include_index=False, allow_formulas=False)
        else:
            sheet_headers = [col.strip() for col in worksheet.row_values(1)]
            df_to_append = pd.DataFrame(columns=sheet_headers)
            for col in sheet_headers:
                if col in newly_added_df.columns: df_to_append[col] = newly_added_df[col]
            worksheet.append_rows(df_to_append.values.tolist(), value_input_option='USER_ENTERED')
        return added_count
    except Exception as e:
        print(f"\n시트 쓰기 중 오류 발생: {e}")
        return -1

def main():
    if not all([SERVICE_KEY, GOOGLE_CREDENTIALS_JSON]):
        print("오류: 필수 환경 변수가 설정되지 않았습니다.")
        return

    print(f"===== 전국 아파트 실거래가 업데이트 시작 (대상 월: {MONTHS_TO_FETCH}) =====")
    
    lawd_codes = get_lawd_codes(LAWD_CODE_FILE)
    if not lawd_codes: return

    session = requests.Session()
    session.mount('https://', CustomHttpAdapter())
    
    creds = get_google_creds()
    if not creds: return
    gc = gspread.service_account_from_dict(creds)

    total_added_count = 0
    
    # [수정] 월별로 루프를 돌면서 처리
    for month in MONTHS_TO_FETCH:
        print(f"\n--- {month} 데이터 처리 시작 ---")
        monthly_data = []
        for i, code in enumerate(lawd_codes):
            print(f"\r  [{i+1}/{len(lawd_codes)}] {code} 수집 중...", end="", flush=True)
            region_data = fetch_data_for_region(session, code, month, SERVICE_KEY)
            if region_data:
                monthly_data.extend(region_data)
            time.sleep(0.1)

        if not monthly_data:
            print(f"\n{month}월에 수집된 데이터가 없습니다.")
            continue

        df_month = pd.DataFrame(monthly_data)
        df_month.columns = df_month.columns.str.strip()
        print(f"\n{month}월 데이터 총 {len(df_month)}건 수집 완료. 구글 시트 업데이트를 시작합니다.")

        # 한 달치 데이터로 시트 업데이트 함수 호출
        added_count = update_google_sheet(df_month, gc)
        
        if added_count > 0:
            total_added_count += added_count
            print(f"{month}월 데이터 중 {added_count}건 신규 추가 완료.")
        elif added_count == -1:
            print(f"{month}월 데이터 처리 중 오류가 발생했습니다.")

    print(f"\n===== 전체 프로세스 완료! 총 {total_added_count}건의 신규 데이터 추가됨 =====")

if __name__ == '__main__':
    main()
