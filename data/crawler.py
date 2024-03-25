import requests as rq
from bs4 import BeautifulSoup
import re
import requests as rq
from io import BytesIO
import pandas as pd
import json
from tqdm import tqdm
import time
from dateutil.relativedelta import relativedelta
from datetime import date

def crawl_latest_trading_day():
    """
    네이버 금융 사이트에서 최근 영업일 정보를 추출합니다.

    반환:
        str: 'YYYYMMDD' 형식의 최근 영업일입니다.
    """
    # 네이버 금융 사이트에서 영업일 정보를 가져오기 위한 URL
    finance_url = 'https://finance.naver.com/sise/sise_deposit.naver'
    data = rq.get(finance_url)
    data_html = BeautifulSoup(data.content, 'html.parser')  # 명확성을 위해 파서 타입 추가

    # 'span#time1' 요소 안에서 날짜 문자열 추출
    parsed_day = data_html.select_one('span.tah').text

    # 정규 표현식을 사용하여 날짜 문자열에서 숫자 부분을 추출하고 합칩니다
    mkt_day = re.findall('[0-9]+', parsed_day)
    mkt_day = ''.join(mkt_day)

    return mkt_day


def crawl_mkt_data(mkt_day):
    """
    주어진 거래일에 대한 KOSPI 혹은 KOSDAQ 시장 데이터를 가져옵니다.

    매개변수:
        mkt_day (str): 'YYYYMMDD' 형식의 시장 거래일.

    반환:
        output_mkt (list): 시장별 데이터프레임이 담긴 리스트입니다.
    """

    mkt = ['STK', 'KSQ', 'ALL']
    output_mkt = []

    # tqdm을 사용하여 진행 상황을 표시하며 모든 섹터 정보 크롤링
    for i in tqdm(mkt):
        # url
        url = 'dbms/MDC/STAT/standard/MDCSTAT03501' if i == 'ALL' else 'dbms/MDC/STAT/standard/MDCSTAT03901'
        
        # OTP를 얻기 위한 URL 및 쿼리 파라미터
        otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        otp_qry = {
            'searchType': '1',
            'mktId': i, # 'STK'는 코스피, 'KSQ'는 코스닥, 'ALL'은 개별종목을 의미합니다.
            'trdDd': mkt_day,
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': url
        }
        headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}

        otp = rq.post(otp_url, otp_qry, headers=headers).text # 파일 다운로드를 위한 OTP 요청

        mkt_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd' # 시장 데이터를 다운로드하기 위한 URL
        down_mkt = rq.post(mkt_url, {'code': otp}, headers=headers)

        df_mkt = pd.read_csv(BytesIO(down_mkt.content), encoding='EUC-KR') # EUC-KR 인코딩으로 되어있는 한국거래소 데이터를 Pandas DataFrame으로 변환
        output_mkt.append(df_mkt) # 변환된 데이터프레임을 리스트에 추가

    return output_mkt

def crawl_sector_data(mkt_day):
    """
    모든 섹터 정보를 크롤링하여 데이터프레임 리스트로 반환합니다.

    매개변수:
        mkt_day (str): 'YYYYMMDD' 형식의 시장 거래일입니다.

    반환:
        output_sector (list): 섹터별 데이터프레임이 담긴 리스트입니다.
    """
    sector_code = ['G25', 'G35', 'G50', 'G10', 'G20', 'G55', 'G30', 'G15', 'G45']
    output_sector = []

    # tqdm을 사용하여 진행 상황을 표시하며 모든 섹터 정보 크롤링
    for i in tqdm(sector_code):
        url = f'https://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={mkt_day}&sec_cd={i}'
        data = rq.get(url).json()
        df_data = pd.json_normalize(data['list'])  # 딕셔너리에서 데이터프레임으로 변환
        output_sector.append(df_data)  # 변환된 데이터프레임을 리스트에 추가
        time.sleep(2)  # 무한 크롤링 방지용 슬립

    return output_sector


def crawl_code_data():
    """
    한국거래소에서 종목코드와 표준코드가 포함된 정보를 크롤링하여 가져옵니다.

    반환:
        output_code (list): 크롤링된 종목코드와 표준코드가 포함된 정보가 담긴 리스트입니다.
    """
    # 크롤링에 사용될 URL과 요청 파라미터 설정
    code_url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    code_qry = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
        "locale": "ko_KR",
        "mktId": "ALL",
        "share": "1",
        "csvxls_isNo": "false"
    }
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}

    # POST 요청을 통해 데이터 가져오기
    data_code = rq.post(code_url, code_qry, headers=headers).content
    decoded_code = data_code.decode('utf-8')
    parsed_code = json.loads(decoded_code)
    
    # 결과 데이터 추출
    output_code = parsed_code['OutBlock_1']

    return output_code

def crawl_price_data(CD_finder, STCD_finder, NM_finder):
    """
    특정 종목에 대한 주가 데이터를 가져옵니다.

    매개변수:
        CD_finder (str): 종목코드입니다.
        STCD_finder (str): 표준코드입니다.
        NM_finder (str): 종목명입니다.

    반환:
        output_data (list): 가져온 주가 데이터가 담긴 리스트입니다.
    """
    # 시작일과 종료일 계산
    fr = (date.today() + relativedelta(years=-5)).strftime("%Y%m%d")
    to = date.today().strftime("%Y%m%d")

    # 주가 데이터 가져오기 위한 URL 및 파라미터 설정
    price_url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    price_qry = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
        'locale': 'ko_KR',
        'tboxisuCd_finder_stkisu0_1': f"{CD_finder}/{NM_finder}",
        'isuCd': STCD_finder,
        'isuCd2': CD_finder,
        'codeNmisuCd_finder_stkisu0_1': NM_finder,
        'param1isuCd_finder_stkisu0_1': 'ALL',
        'strtDd': fr,
        'endDd': to,
        'adjStkPrc_check': 'Y',
        'adjStkPrc': '2',
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false'
    }
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}

    # 데이터 가져오기
    response = rq.post(price_url, data=price_qry, headers=headers)
    parsed_data = response.json()
    output_data = parsed_data['output']

    return output_data


def crawl_financial_data(code):
    """
    주어진 종목코드에 대한 재무 데이터를 긁어와 연간 및 분기 재무 데이터프레임을 반환합니다.

    매개변수:
        code (str): 종목코드입니다.

    반환:
        data_fs_y (DataFrame): 연간 재무 데이터가 담긴 데이터 프레임입니다.
        data_fs_q (DataFrame): 분기 재무 데이터가 담긴 데이터 프레임입니다.
    """
    # url 생성
    url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{code}'

    # 데이터 받아오기
    data = pd.read_html(url, displayed_only=False)

    # 연간 데이터 처리
    data_fs_y = pd.concat([
        data[0].iloc[:, ~data[0].columns.str.contains('전년동기')], data[2], data[4]
    ])
    data_fs_y = data_fs_y.rename(columns={data_fs_y.columns[0]: "계정"})

    # 결산년 찾기 및 데이터 필터링
    page_data = rq.get(url)
    page_data_html = BeautifulSoup(page_data.content, 'html.parser')
    fiscal_data = page_data_html.select('div.corp_group1 > h2')
    fiscal_data_text = fiscal_data[1].text
    fiscal_data_text = re.findall('[0-9]+', fiscal_data_text)
    data_fs_y = data_fs_y.loc[:, (data_fs_y.columns == '계정') | (
        data_fs_y.columns.str[-2:].isin(fiscal_data_text))]

    # 분기 데이터 처리
    data_fs_q = pd.concat([
        data[1].iloc[:, ~data[1].columns.str.contains('전년동기')], data[3], data[5]
    ])
    data_fs_q = data_fs_q.rename(columns={data_fs_q.columns[0]: "계정"})

    return data_fs_y, data_fs_q