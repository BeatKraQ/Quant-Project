import requests
import json
import keyring
import pandas as pd
import time
from datetime import timedelta
import pandas as pd


# key
app_key = keyring.get_password('mock_app_key', 'stanfm')
app_secret = keyring.get_password('mock_app_secret', 'stanfm')

url_base = "https://openapivts.koreainvestment.com:29443" # 모의투자

def get_access_token(url_base, app_key, app_secret):
    """
    API 액세스 토큰을 얻기 위한 함수입니다.

    매개변수:
        app_key (str): API 사용자의 app key입니다.
        app_secret (str): API 사용자의 app secret입니다.

    반환:
        access_token (str): API 호출에 사용될 액세스 토큰입니다.
    """
    headers = {"content-type": "application/json"}
    path = "oauth2/tokenP"
    
    # 요청 본문에 필요한 정보를 담습니다.
    body = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecret": app_secret
    }
    
    # 전체 URL을 구성합니다.
    url = f"{url_base}/{path}"
    
    # POST 요청을 보내고 응답을 받습니다.
    res = requests.post(url, headers=headers, data=json.dumps(body))
    
    # 응답으로부터 액세스 토큰을 추출합니다.
    access_token = res.json()['access_token']
    
    return access_token

def hashkey(url_base, app_key, app_secret, datas):
    """
    주어진 정보를 사용하여 API에 요청을 보내고 해시키를 받아오는 함수입니다.

    매개변수:
        url_base (str): API의 기본 URL입니다.
        app_key (str): API 접근을 위한 애플리케이션 키입니다.
        app_secret (str): API 접근을 위한 애플리케이션 비밀 키입니다.
        datas (dict): 해시키를 생성하기 위한 데이터를 담고 있는 딕셔너리입니다.

    반환:
        hashkey (str): 응답으로 받은 해시키입니다.
    """
    # API 경로 설정
    path = "uapi/hashkey"
    
    # 전체 URL 구성
    url = f"{url_base}/{path}"
    
    # 요청 헤더 구성
    headers = {
        'content-Type': 'application/json',
        'appKey': app_key,
        'appSecret': app_secret,
    }
    
    # POST 요청 보내기 및 응답 받기
    res = requests.post(url, headers=headers, data=json.dumps(datas))
    
    # 응답으로부터 해시키 추출
    hashkey = res.json()["HASH"]

    return hashkey

# 현재가 구하기
def get_price(url_base, app_key, app_secret, code):
    """
    주식의 현재가를 조회하는 함수입니다.

    매개변수:
        url_base (str): API의 기본 URL입니다.
        app_key (str): API 접근을 위한 애플리케이션 키입니다.
        app_secret (str): API 접근을 위한 애플리케이션 비밀 키입니다.
        code (str): 조회할 주식의 종목 코드입니다.

    반환:
        price (int): 조회된 주식의 현재가입니다.
    """
    # API 경로 설정
    path = "uapi/domestic-stock/v1/quotations/inquire-price"
    
    # 전체 URL 구성
    url = f"{url_base}/{path}"
    
    # 액세스 토큰 가져오기
    access_token = get_access_token(url_base, app_key, app_secret)
    
    # 요청 헤더 구성
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": app_key,
        "appSecret": app_secret,
        "tr_id": "FHKST01010100"
    }
    
    # 요청 파라미터 설정
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
    
    # GET 요청 보내기 및 응답 받기
    res = requests.get(url, headers=headers, params=params)
    
    # 응답으로부터 주식 현재가 추출 및 정수형 변환
    price = res.json()['output']['stck_prpr']
    price = int(price)
    
    # 요청 간격 유지
    time.sleep(0.1)

    return price

# 주문
def trading(url_base, app_key, app_secret, code, tr_id):
    """
    주식을 매수하거나 매도하는 주문을 실행하는 함수입니다.

    매개변수:
        url_base (str): API의 기본 URL입니다.
        app_key (str): API 접근을 위한 애플리케이션 키입니다.
        app_secret (str): API 접근을 위한 애플리케이션 비밀 키입니다.
        code (str): 주문할 주식의 종목 코드입니다.
        tr_id (str): 매수 또는 매도 방법에 대한 코드입니다. 이 코드는 거래의 유형(매수, 매도 등)을 결정합니다.

    반환:
        없음. 결과는 직접 확인해야 합니다. 함수는 API로 주문 요청을 보내고, 그 응답을 처리하지만 반환 값은 제공하지 않습니다.
    """
    # API 경로 설정
    path = "/uapi/domestic-stock/v1/trading/order-cash"
    
    # 전체 URL 구성
    url = f"{url_base}/{path}"
    
    # 액세스 토큰 가져오기
    access_token = get_access_token(url_base, app_key, app_secret)
    
    # 주문 데이터 구성
    data = {
        "CANO": "50102599",  # 계좌번호(앞 8자리)
        "ACNT_PRDT_CD": "01",  # 계좌 상품 코드
        "PDNO": code,  # 상품 번호(종목 코드)
        "ORD_DVSN": "03",  # 주문 구분 (예: 지정가, 시장가 등)
        "ORD_QTY": "1",  # 주문 수량
        "ORD_UNPR": "0",  # 주문 단가
    }
    
    # 요청 헤더 구성
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appKey": app_key,
        "appSecret": app_secret,
        "tr_id": tr_id,
        "custtype": "P",
        "hashkey": hashkey(url_base, app_key, app_secret, data)  # 해시키 생성
    }
    
    # POST 요청을 보내고 응답을 받음
    res = requests.post(url, headers=headers, data=json.dumps(data))


# 계좌 잔고 조회
def check_account(url_base, app_key, app_secret, access_token):
    """
    계좌 잔고를 조회하는 함수입니다.

    매개변수:
        url_base (str): API의 기본 URL입니다.
        app_key (str): API 접근을 위한 애플리케이션 키입니다.
        app_secret (str): API 접근을 위한 애플리케이션 비밀 키입니다.
        access_token (str): API 접근을 위한 액세스 토큰입니다.

    반환:
        [res1, res2] (list): 조회된 계좌 잔고 정보를 담은 데이터 프레임 리스트입니다. res1은 종목별 보유수량, res2는 추가 잔고 정보입니다. (FYI: 잔고조회 API는 모의투자에서는 한번에 20종목까지, 실제계좌에서는 50종목까지 조회가 가능)
    """
    output1 = []
    output2 = []
    CTX_AREA_NK100 = ''

    # 액세스 토큰 새로 가져오기 (필요한 경우)
    access_token = get_access_token(url_base, app_key, app_secret)

    while True:
        # API 경로 설정
        path = "/uapi/domestic-stock/v1/trading/inquire-balance"
        url = f"{url_base}/{path}"

        # 요청 헤더 구성
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appKey": app_key,
            "appSecret": app_secret,
            "tr_id": "VTTC8434R"
        }

        # 요청 파라미터 설정
        params = {
            "CANO": "50102599",  # 계좌번호(앞 8자리)
            "ACNT_PRDT_CD": "01",  # 계좌 상품 코드
            # 추가 파라미터 설정
        }

        # GET 요청 보내기 및 응답 받기
        res = requests.get(url, headers=headers, params=params)
        
        # 응답 데이터 추가
        output1.append(pd.DataFrame.from_records(res.json()['output1']))

        # 다음 페이지 정보 업데이트
        CTX_AREA_NK100 = res.json()['ctx_area_nk100'].strip()

        # 마지막 페이지인 경우 반복 중지
        if CTX_AREA_NK100 == '':
            output2.append(res.json()['output2'][0])
            break

    # 결과 데이터 프레임 처리
    if not output1[0].empty:
        res1 = pd.concat(output1)[['pdno', 'hldg_qty']].rename(columns={'pdno': '종목코드', 'hldg_qty': '보유수량'}).reset_index(drop=True)
    else:
        res1 = pd.DataFrame(columns=['종목코드', '보유수량'])

    res2 = output2[0]

    return [res1, res2]
