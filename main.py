import os
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from urllib.parse import unquote
import xml.etree.ElementTree as ET
import time
# --- 아래 3줄 추가 ---
import ssl
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

# --- 1. 설정 (Configuration) ---

# GitHub Secrets에서 환경 변수를 통해 키를 불러옵니다.
SERVICE_KEY = os.getenv('SERVICE_KEY')

# 구글 서비스 계정 키 파일 경로 (GitHub Actions에서 생성됨)
GOOGLE_CREDENTIALS_PATH = 'credentials.json'

# 데이터를 업로드할 구글 시트 이름
GOOGLE_SHEET_NAME = '전국 아파트 매매 실거래가_누적' 

# 법정동 코드 파일 경로
LAWD_CODE_FILE = 'lawd_code.csv'

# API 기본 URL
BASE_URL = 'https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev'

# !!! 중요: 조회하고 싶은 '계약년월' 목록을 여기에 추가하세요. !!!
# 예: ['202401'] -> 1월 데이터를 가져옵니다.
# 테스트를 위해서는 실제 데이터가 있는 과거 날짜로 설정하세요.
MONTHS_TO_FETCH = ['202401'] 

# --- 아래 클래스 추가 ---
# 보안 프로토콜을 TLSv1.2로 강제하기 위한 어댑터 클래스 정의
class TLSv1_2Adapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLSv1_2,
        )

# --- 2. 함수 정의 (Functions) ---

def get_lawd_codes(filepath):
    """CSV 파일에서 법정동 코드 목록을 읽어옵니다."""
    try:
        df = pd.read_csv(filepath)
        print(f"총 {len(df['code'])}개의 지역 코드를 불러왔습니다.")
        return df['code'].astype(str).tolist()
    except FileNotFoundError:
        print(f"오류: {filepath} 파일을 찾을 수 없습니다.")
        return []

# --- 'session' 인자를 받도록 함수 수정 ---
def fetch_data_for_region(session, lawd_cd, deal_ymd, service_key):
    """특정 지역, 특정 월의 데이터를 API로부터 가져옵니다."""
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
            # --- requests.get을 session.get으로 변경 ---
            response = session.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            result_code = root.find('header/resultCode').text
            
            if result_code != '00':
                if result_code != '04':
                    print(f"  [API 응답 오류] 지역코드: {lawd_cd}, 코드: {result_code}, 메시지: {root.find('header/resultMsg').text}")
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
    """데이터프레임에 고유 ID 열을 생성합니다."""
    # 고유 식별을 위한 컬럼 목록 (API 응답 명세서 기준)
    id_cols = [
        '일련번호', '거래금액', '년', '월', '일', 
        '전용면적', '지번', '층', '법정동'
    ]
    
    # 데이터프레임에 존재하는 컬럼만으로 ID 생성
    valid_cols = [col for col in id_cols if col in df.columns]
    if not valid_cols:
        print("고유 ID를 생성할 컬럼이 부족합니다.")
        return None

    # 모든 값을 문자열로 변환하여 합치기
    df['unique_id'] = df[valid_cols].astype(str).apply(lambda x: '_'.join(x), axis=1)
    return df


def main():
    """전체 프로세스를 실행하는 메인 함수"""
    print("="*50)
    print("전국 아파트 실거래가 누적 업데이트를 시작합니다.")
    print(f"대상 월: {MONTHS_TO_FETCH}")
    print("="*50)
    
    # 1. API로 새로운 데이터 가져오기
    lawd_codes = get_lawd_codes(LAWD_CODE_FILE)
    if not lawd_codes: return

    all_new_data = []
    
    # --- 아래 3줄 코드 추가 ---
    # TLSv1.2를 사용하는 세션 생성
    session = requests.Session()
    session.mount('https://', TLSv1_2Adapter())

    total_regions = len(lawd_codes)
    for month in MONTHS_TO_FETCH:
        print(f"\n--- {month} 데이터 수집 시작 ---")
        for i, code in enumerate(lawd_codes):
            print(f"\r  [{i+1}/{total_regions}] {code} 수집 중...", end="")
            # --- 함수 호출 시 session 전달하도록 수정 ---
            region_data = fetch_data_for_region(session, code, month, SERVICE_KEY)
            if region_data:
                all_new_data.extend(region_data)
        print("\n--- 수집 완료 ---")
    
    if not all_new_data:
        print("\nAPI로부터 수집된 새로운 데이터가 없습니다. 프로세스를 종료합니다.")
        return

    df_new = pd.DataFrame(all_new_data)
    print(f"\nAPI로부터 총 {len(df_new)}건의 데이터를 수집했습니다.")

    # 2. 구글 시트에서 기존 데이터 읽기
    print("\n구글 시트에서 기존 데이터를 읽어옵니다...")
    try:
        gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_PATH)
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.get_worksheet(0)
        existing_records = worksheet.get_all_records()
        df_existing = pd.DataFrame(existing_records)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"경고: '{GOOGLE_SHEET_NAME}' 시트를 찾을 수 없습니다. 시트가 비어있는 것으로 간주하고 진행합니다.")
        df_existing = pd.DataFrame()
    except Exception as e:
        print(f"구글 시트 읽기 중 오류 발생: {e}")
        return

    # 3. 데이터 비교 및 결합
    if not df_existing.empty:
        pass

    df_new = create_unique_id(df_new)
    if not df_existing.empty:
        df_existing = create_unique_id(df_existing)
    
    if not df_existing.empty:
        newly_added_df = df_new[~df_new['unique_id'].isin(df_existing['unique_id'])]
    else:
        newly_added_df = df_new
    
    if newly_added_df.empty:
        print("\n추가할 새로운 거래 데이터가 없습니다. 프로세스를 종료합니다.")
        return
    
    print(f"\n총 {len(newly_added_df)}건의 신규 데이터를 확인했습니다. 시트에 추가합니다.")
    
    if 'unique_id' in newly_added_df.columns:
        newly_added_df.drop(columns=['unique_id'], inplace=True)
        
    # 4. 최종 데이터를 구글 시트에 추가 (Append)
    print("\n신규 데이터를 시트 마지막에 추가합니다...")
    # 시트가 비어있을 경우 헤더를 포함하여 데이터를 쓰고, 그렇지 않으면 데이터만 추가
    if df_existing.empty:
        set_with_dataframe(worksheet, newly_added_df, include_index=False, allow_formulas=False)
    else:
        worksheet.append_rows(
            newly_added_df.values.tolist(), 
            value_input_option='USER_ENTERED'
        )
    
    print("="*50)
    print("구글 시트 업데이트가 성공적으로 완료되었습니다!")
    print(f"추가된 데이터 건수: {len(newly_added_df)}")
    print("="*50)


# --- 3. 메인 실행 로직 ---
if __name__ == '__main__':
    if not SERVICE_KEY:
        print("오류: SERVICE_KEY 환경 변수가 설정되지 않았습니다.")
        print("GitHub Actions Secrets에 SERVICE_KEY를 등록했는지 확인해주세요.")
    else:
        main()
