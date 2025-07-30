import os
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from urllib.parse import unquote
import xml.etree.ElementTree as ET
import time
import ssl
from requests.adapters import HTTPAdapter

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

SERVICE_KEY = os.getenv('SERVICE_KEY')
GOOGLE_CREDENTIALS_PATH = 'credentials.json'
GOOGLE_SHEET_NAME = '전국 아파트 매매 실거래가_누적' 
LAWD_CODE_FILE = 'lawd_code.csv'
BASE_URL = 'https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev'
MONTHS_TO_FETCH = ['202501', '202502', '202503', '202504', '202505', '202506', '202507', '202508', '202509', '202510', '202511', '202512']

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
    page_no = 1
    
    while True:
        params = {
            'serviceKey': service_key,
            'LAWD_CD': lawd_cd,
            'DEAL_YMD': deal_ymd,
            'pageNo': str(page_no),
            'numOfRows': '1000'
        }
        try:
            response = session.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            root = ET.fromstring(response.content)

            header = root.find('header')
            if header is None:
                print(f"  [API 응답 형식 오류] 지역코드: {lawd_cd}, 응답에 header가 없습니다.")
                break
            
            result_code_element = header.find('resultCode')
            if result_code_element is None:
                print(f"  [API 응답 형식 오류] 지역코드: {lawd_cd}, 응답에 resultCode가 없습니다.")
                break
                
            result_code = result_code_element.text
            
            if result_code != '000':
                if result_code not in ['04', '22']:
                    msg_element = header.find('resultMsg')
                    msg = msg_element.text if msg_element is not None else "메시지 없음"
                    print(f"  [API 응답 오류] 지역코드: {lawd_cd}, 코드: {result_code}, 메시지: {msg}")
                break
            
            items_element = root.find('body/items')
            if items_element is None or not items_element:
                break
            
            current_page_items = items_element.findall('item')
            if not current_page_items:
                break
            
            for item in current_page_items:
                item_dict = {child.tag: child.text for child in item}
                all_items.append(item_dict)
            
            page_no += 1
            time.sleep(0.2)
        except requests.exceptions.RequestException as e:
            print(f"  [네트워크 오류] 지역코드: {lawd_cd}, 오류: {e}")
            break
        except ET.ParseError as e:
            print(f"  [XML 파싱 오류] 지역코드: {lawd_cd}, 응답 내용: {response.text[:200]}")
            break

    return all_items

def create_unique_id(df):
    if df is None or df.empty:
        return df
    id_cols = ['aptSeq', 'dealAmount', 'dealYear', 'dealMonth', 'dealDay', 'excluUseAr', 'jibun', 'floor', 'sggCd', 'umdCd']
    valid_cols = [col for col in id_cols if col in df.columns]
    if not valid_cols:
        print("고유 ID를 생성할 컬럼이 부족하여 ID 생성을 건너뜁니다.")
        return df
    print(f"고유 ID 생성을 위해 사용되는 컬럼: {valid_cols}")
    df['unique_id'] = df[valid_cols].astype(str).agg('_'.join, axis=1)
    return df

def main():
    print("="*50)
    print("전국 아파트 실거래가 누적 업데이트를 시작합니다.")
    print(f"대상 월: {MONTHS_TO_FETCH}")
    print("="*50)
    
    lawd_codes = get_lawd_codes(LAWD_CODE_FILE)
    if not lawd_codes: return

    all_new_data = []
    
    session = requests.Session()
    session.mount('https://', CustomHttpAdapter())

    total_regions = len(lawd_codes)
    for month in MONTHS_TO_FETCH:
        print(f"\n--- {month} 데이터 수집 시작 ---")
        for i, code in enumerate(lawd_codes):
            print(f"\r  [{i+1}/{total_regions}] {code} 수집 중...", end="")
            region_data = fetch_data_for_region(session, code, month, SERVICE_KEY)
            if region_data:
                all_new_data.extend(region_data)
        print("\n--- 수집 완료 ---")
    
    if not all_new_data:
        print("\nAPI로부터 수집된 새로운 데이터가 없습니다. 프로세스를 종료합니다.")
        return

    df_new = pd.DataFrame(all_new_data)
    print(f"\nAPI로부터 총 {len(df_new)}건의 데이터를 수집했습니다.")

    print("\n구글 시트에서 기존 데이터를 읽어옵니다...")
    worksheet = None  #<-- 변수를 미리 선언
    df_existing = pd.DataFrame() #<-- 변수를 미리 선언
    try:
        gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_PATH)
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.get_worksheet(0)
        existing_records = worksheet.get_all_records()
        df_existing = pd.DataFrame(existing_records)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"경고: '{GOOGLE_SHEET_NAME}' 시트를 찾을 수 없습니다. 시트가 비어있는 것으로 간주하고 진행합니다.")
        # worksheet 변수는 None으로 유지됨
    except Exception as e:
        print(f"구글 시트 읽기 중 오류 발생: {e}")
        return

    df_new = create_unique_id(df_new)
    if not df_existing.empty:
        df_existing = create_unique_id(df_existing)
    
    if not df_existing.empty:
        if 'unique_id' in df_new.columns and 'unique_id' in df_existing.columns:
            newly_added_df = df_new[~df_new['unique_id'].isin(df_existing['unique_id'])]
        else:
            print("경고: 고유 ID가 없어 중복 제거를 건너뛰고 모든 새 데이터를 추가합니다.")
            newly_added_df = df_new
    else:
        newly_added_df = df_new
    
    if newly_added_df is None or newly_added_df.empty:
        print("\n추가할 새로운 거래 데이터가 없습니다. 프로세스를 종료합니다.")
        return
    
    print(f"\n총 {len(newly_added_df)}건의 신규 데이터를 확인했습니다. 시트에 추가합니다.")
    
    if 'unique_id' in newly_added_df.columns:
        newly_added_df = newly_added_df.drop(columns=['unique_id'])
        
    print("\n신규 데이터를 시트 마지막에 추가합니다...")
    # --- worksheet가 없을 경우에 대한 처리 추가 ---
    if worksheet is None:
        try:
            # 시트가 없었으므로 새로 만들고 데이터를 씀
            sh = gc.create(GOOGLE_SHEET_NAME)
            # 서비스 계정에 권한을 줘야 다음 실행부터 접근 가능
            sh.share(os.getenv('GSPREAD_SERVICE_ACCOUNT_EMAIL', '서비스 계정 이메일을 Secrets에 등록하세요'), perm_type='user', role='writer')
            worksheet = sh.get_worksheet(0)
            set_with_dataframe(worksheet, newly_added_df, include_index=False, allow_formulas=False)
        except Exception as e:
            print(f"새 시트 생성 및 쓰기 중 오류 발생: {e}")
            print("팁: 구글 드라이브에서 '전국 아파트 실거래가_누적' 파일을 직접 만들고 서비스 계정에 공유해주세요.")
            return
    elif df_existing.empty:
        # 시트는 있었지만 비어있는 경우
        set_with_dataframe(worksheet, newly_added_df, include_index=False, allow_formulas=False)
    else:
        try:
            sheet_headers = list(df_existing.columns)
            if 'unique_id' in sheet_headers:
                sheet_headers.remove('unique_id')
            newly_added_df_aligned = newly_added_df[sheet_headers]
            worksheet.append_rows(
                newly_added_df_aligned.values.tolist(), 
                value_input_option='USER_ENTERED'
            )
        except Exception:
            worksheet.append_rows(
                newly_added_df.values.tolist(), 
                value_input_option='USER_ENTERED'
            )
    
    print("="*50)
    print("구글 시트 업데이트가 성공적으로 완료되었습니다!")
    print(f"추가된 데이터 건수: {len(newly_added_df)}")
    print("="*50)

if __name__ == '__main__':
    if not SERVICE_KEY:
        print("오류: SERVICE_KEY 환경 변수가 설정되지 않았습니다.")
        print("GitHub Actions Secrets에 SERVICE_KEY를 등록했는지 확인해주세요.")
    else:
        main()
