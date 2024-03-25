import datetime
import keyring
import schedule
import numpy as np
import pandas as pd
from datetime import timedelta
import warnings
from trading.trading import get_access_token, check_account, get_price, trading

# 스크립트의 메인 실행 함수
def main():
    warnings.filterwarnings(action='ignore')

    app_key = keyring.get_password('mock_app_key', 'stanfm')
    app_secret = keyring.get_password('mock_app_secret', 'stanfm')
    url_base = "https://openapivts.koreainvestment.com:29443"

    access_token = get_access_token(url_base, app_key, app_secret)
    mp = pd.read_excel('my_portfolio.xlsx', dtype=str)
    ap, account = check_account(url_base, app_key, app_secret, access_token)
    invest_per_stock = int(account['tot_evlu_amt']) * 0.98 / len(mp)

    target = mp.merge(ap, on='종목코드', how='outer')
    target['보유수량'] = target['보유수량'].fillna(0).astype(int)
    target['현재가'] = target.apply(lambda x: get_price(url_base, app_key, app_secret, x['종목코드']), axis=1)
    target['목표수량'] = np.where(target['종목코드'].isin(mp['종목코드']), round(invest_per_stock / target['현재가']), 0)
    target['투자수량'] = target['목표수량'] - target['보유수량']

    schedule_trading(target, url_base, app_key, app_secret, access_token)

# 매매를 위한 스케쥴 설정 및 매매 로직을 실행
def schedule_trading(target, url_base, app_key, app_secret, access_token):
    startDt, endDt = get_trading_hours()
    schedule.clear()
    
    for t in range(target.shape[0]):
        schedule_orders(target, t, url_base, app_key, app_secret, access_token, startDt, endDt)
        
    while datetime.datetime.now() < endDt:
        schedule.run_pending()

    print('Trading session finished.')
    schedule.clear()

# 매매 가능 시간을 정의
def get_trading_hours():
    startDt1 = datetime.datetime.now() + timedelta(minutes=1)
    startDt2 = datetime.datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)
    startDt = max(startDt1, startDt2)
    endDt = datetime.datetime.now().replace(hour=15, minute=30, second=0, microsecond=0)
    return startDt, endDt

# 주문을 시간별로 스케줄링
def schedule_orders(target, t, url_base, app_key, app_secret, startDt, endDt):
    n = target.loc[t, '투자수량']
    position = 'VTTC0802U' if n > 0 else 'VTTC0801U'
    code = target.loc[t, '종목코드']
    time_list = pd.date_range(startDt, endDt, periods=abs(n)).round('s')
    
    for schedule_time in time_list:
        schedule_time_str = schedule_time.strftime('%H:%M:%S')
        schedule.every().day.at(schedule_time_str).do(trading, url_base, app_key, app_secret, code, position)

if __name__ == "__main__":
    main()