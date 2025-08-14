# <<< 최종 버전 main.py (최근 3개월 수집) >>>

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
from dateutil.relativedelta import relativedelta # 최근 3개월 계산을 위해 추가

class CustomHttpAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = requests.urllib3.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context
        )

# --- 설정: GitHub Secrets에서 환경 변수 읽기 ---
SERVICE_KEY = os.getenv('SERVICE_KEY')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
GOOGLE_SHEET_NAME = '전국 아파트 매매 실거래가_누적'
LAWD_CODE_FILE = 'lawd_code.csv'
BASE_URL = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'

# --- 설정: 수집할 연월 동적 생성 (최근 3개월) ---
# 매일 실행되므로, 현재 월을 포함한 최근 3개월치 데이터를 수집하도록 설정합니다.
# GitHub Actions는 UTC 기준이므로, 한국 시간(KST)에 맞춰 9시간을 더합니다.
today_kst = datetime.utcnow() + timedelta(hours=9)
MONTHS_TO_FETCH = []
for i in range(3): # 0, 1, 2 (총 3번 반복)
    # 현재 달, 한 달 전, 두 달 전 날짜를 계산
    target_date = today_kst - relativedelta(months=i)
    MONTHS_TO_FETCH.append(target_date.strftime('%Y%m'))


def get_google_creds():
    """GitHub Secrets에서 가져온 JSON 문자열을 딕셔너리로 변환"""
    if GOOGLE_CREDENTIALS_JSON is None:
        print("오류: GOOGLE_CREDENTIALS_JSON 환경 변수가 설정되지 않았습니다.")
        return None
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        return creds_dict
    except json.JSONDecodeError:
        print("오류: GOOGLE_CREDENTIALS_JSON의 형식이 올바르지 않습니다.")
        return None

def get_lawd_codes(filepath):
    try:
        df = pd.read_csv(filepath)
        print(f"총 {len(df['code'])}개의 지역 코드를 불러왔습니다.")
        return df['code'].astype(str).tolist()
    except FileNotFoundError:
        print(f"오류: {filepath} 파일을 찾을 수 없습니다.")
        return []

def fetch_data_for_region(session, lawd_cd, deal_ymd, service_key):
    all_items = []
    params = {
        'serviceKey': service_key,
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ymd,
        'pageNo': '1',
        'numOfRows': '5000' # 최대치로 설정
    }
    try:
        response = session.get(BASE_URL, params=params, timeout=60)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        result_code_element = root.find('header/resultCode')
        if result_code_element is None or result_code_element.text != '00':
            msg_element = root.find('header/resultMsg')
            msg = msg_element.text if msg_element is not None else "메시지 없음"
            if result_code_element is None or result_code_element.text != '99': # 데이터 없는 정상 오류는 출력 안함
                 print(f"  [API 응답 오류] 지역코드: {lawd_cd}, 코드: {result_code_element.text if result_code_element is not None else 'N/A'}, 메시지: {msg}")
            return []

        items_element = root.find('body/items')
        if items_element is None: return []
        
        for item in items_element.findall('item'):
            item_dict = {child.tag: child.text.strip() if child.text else '' for child in item}
            all_items.append(item_dict)
        
        time.sleep(0.1)
    except requests.exceptions.RequestException as e:
        print(f"  [네트워크 오류] 지역코드: {lawd_cd}, 오류: {e}")
    except ET.ParseError:
        print(f"  [XML 파싱 오류] 지역코드: {lawd_cd}")
    return all_items

def create_unique_id(df):
    if df.empty: return df
    id_cols = ['거래금액', '년', '월', '일', '전용면적', '지번', '층', '법정동시군구코드', '법정동읍면동코드']
    valid_cols = [col for col in id_cols if col in df.columns]
    df['unique_id'] = df[valid_cols].astype(str).agg('_'.join, axis=1)
    return df

def main():
    if not all([SERVICE_KEY, GOOGLE_CREDENTIALS_JSON]):
        print("오류: 필수 환경 변수(SERVICE_KEY, GOOGLE_CREDENTIALS_JSON)가 설정되지 않았습니다.")
        return

    print(f"===== 전국 아파트 실거래가 업데이트 시작 (대상 월: {MONTHS_TO_FETCH}) =====")
    
    lawd_codes = get_lawd_codes(LAWD_CODE_FILE)
    if not lawd_codes: return

    session = requests.Session()
    session.mount('http://', CustomHttpAdapter())

    all_new_data = []
    total_regions = len(lawd_codes)
    processed_regions = 0
    for month in MONTHS_TO_FETCH:
        print(f"\n--- {month} 데이터 수집 시작 ---")
        for code in lawd_codes:
            processed_regions += 1
            print(f"\r  [{processed_regions}/{total_regions*len(MONTHS_TO_FETCH)}] {code} 수집 중...", end="", flush=True)
            region_data = fetch_data_for_region(session, code, month, SERVICE_KEY)
            if region_data:
                all_new_data.extend(region_data)
    
    if not all_new_data:
        print("\n\nAPI로부터 수집된 새로운 데이터가 없습니다. 프로세스를 종료합니다.")
        return

    df_new = pd.DataFrame(all_new_data)
    df_new.columns = df_new.columns.str.strip()
    print(f"\n\nAPI로부터 총 {len(df_new)}건의 데이터를 수집했습니다.")

    creds = get_google_creds()
    if not creds: return
    
    try:
        gc = gspread.service_account_from_dict(creds)
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
        worksheet = None # 새로 만들어야 함을 표시
    except Exception as e:
        print(f"구글 시트 처리 중 오류 발생: {e}")
        return

    df_new = create_unique_id(df_new)
    if not df_existing.empty:
        df_existing = create_unique_id(df_existing)
        newly_added_df = df_new[~df_new['unique_id'].isin(df_existing['unique_id'])].copy()
    else:
        newly_added_df = df_new.copy()

    if newly_added_df.empty:
        print("\n추가할 새로운 거래 데이터가 없습니다. 프로세스를 종료합니다.")
        return

    print(f"\n총 {len(newly_added_df)}건의 신규 데이터를 확인했습니다. 시트에 추가합니다.")
    if 'unique_id' in newly_added_df.columns:
        newly_added_df.drop(columns=['unique_id'], inplace=True)

    try:
        if worksheet is None: # 시트가 없어서 새로 만들어야 하는 경우
            sh = gc.create(GOOGLE_SHEET_NAME)
            worksheet = sh.get_worksheet(0)
            service_account_email = os.getenv('GSPREAD_SERVICE_ACCOUNT_EMAIL')
            if service_account_email:
                sh.share(service_account_email, perm_type='user', role='writer')
            set_with_dataframe(worksheet, newly_added_df, include_index=False, allow_formulas=False)
        elif df_existing.empty: # 시트는 있지만 비어있는 경우
            set_with_dataframe(worksheet, newly_added_df, include_index=False, allow_formulas=False)
        else: # 기존 데이터가 있는 경우
            # 기존 시트 헤더 순서에 새 데이터를 맞춤
            sheet_headers = [col.strip() for col in worksheet.row_values(1)]
            df_to_append = pd.DataFrame(columns=sheet_headers)
            for col in sheet_headers:
                if col in newly_added_df.columns:
                    df_to_append[col] = newly_added_df[col]
            worksheet.append_rows(df_to_append.values.tolist(), value_input_option='USER_ENTERED')
    except Exception as e:
        print(f"\n시트 쓰기 중 오류 발생: {e}")
        return

    print(f"\n===== 구글 시트 업데이트 완료! (신규 {len(newly_added_df)}건) =====")

if __name__ == '__main__':
    main()
