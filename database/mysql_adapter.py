import pymysql
import os 
import pandas as pd
import time
from tqdm import tqdm
from mysql_reader import create_db_engine, fetch_kr_code, fetch_latest_base, fetch_quarterly_financials
from data.cleanser import process_market_data, process_code_data, process_sector_data, process_price_data, process_financial_data, calculate_value_indicators
from data.crawler import crawl_mkt_data, crawl_sector_data, crawl_code_data, crawl_price_data, crawl_financial_data

def create_db_connection(db):
    """
    데이터베이스 연결 객체와 커서 객체를 생성하여 반환합니다.

    매개변수:
        db (str): 연결할 데이터베이스의 이름입니다. 'stock'과 같은 문자열을 예상합니다.

    반환:
        con (Connection): PyMySQL을 사용하여 생성된 데이터베이스 연결 객체입니다.
        Cursor (Cursor): 생성된 데이터베이스 연결을 위한 커서 객체입니다.
    """
    # 환경변수에서 데이터베이스 암호 가져오기
    db_password = os.getenv('pswd2')
    
    # 데이터베이스 연결 설정
    con = pymysql.connect(user='root', passwd=db_password, host='127.0.0.1', db=db, charset='utf8')
    
    # 커서 객체 생성
    cursor = con.cursor()
    
    return con, cursor 



def upsert_kr_base(mkt_day):
    """
    주어진 최근 거래일에 대한 국내 주식시장 기본 정보를 크롤링하고, 
    이를 클린징 처리하여 MySQL 데이터베이스의 kr_base 테이블에 삽입하거나 업데이트합니다.

    매개변수:
        mkt_day (str): 최근 거래일을 나타내는 문자열입니다. 'YYYYMMDD' 형식이어야 합니다.
    """
    # 데이터베이스 연결 생성
    con, mycursor = create_db_connection(db='stock')

    # 시장 데이터 크롤링: 최근 거래일에 해당하는 데이터 크롤링
    data = crawl_mkt_data(mkt_day)

    # 시장 데이터 처리: 크롤링된 데이터를 가공 처리
    kr_base = process_market_data(data, mkt_day)

    # SQL 쿼리 설정: 데이터베이스에 존재하지 않는 경우 새로운 레코드로 삽입, 존재하는 경우 업데이트
    query = """
        INSERT INTO kr_base (종목코드, 종목명, 시장구분, 종가, 시가총액, 기준일, EPS, 선행EPS, BPS, 주당배당금, 종목구분)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
        종목명=new.종목명, 시장구분=new.시장구분, 종가=new.종가, 시가총액=new.시가총액,
        EPS=new.EPS, 선행EPS=new.선행EPS, BPS=new.BPS, 주당배당금=new.주당배당금, 종목구분=new.종목구분;
    """

    # 처리된 데이터 프레임에서 SQL 쿼리 실행을 위한 인자 리스트 추출
    args = kr_base.values.tolist()

    # SQL 쿼리 실행: 데이터베이스에 데이터 삽입 또는 업데이트
    mycursor.executemany(query, args)

    # 데이터베이스에 변경 사항 커밋
    con.commit()

    # 데이터베이스 연결 종료
    con.close()


def upsert_kr_sector(mkt_day):
    """
    주어진 최근 거래일에 대한 국내 주식시장 섹터 정보를 크롤링하고, 
    이를 처리하여 MySQL 데이터베이스의 kr_sector 테이블에 삽입하거나 업데이트합니다.

    매개변수:
        mkt_day (str): 최근 거래일을 나타내는 문자열입니다. 'YYYYMMDD' 형식이어야 합니다.
    """
    # 데이터베이스 연결 생성
    con, mycursor = create_db_connection(db='stock')

    # 섹터 데이터 크롤링
    data = crawl_sector_data(mkt_day)

    # 섹터 데이터 처리
    kr_sector = process_sector_data(data, mkt_day)

    # SQL 쿼리 설정: 데이터베이스에 존재하지 않는 경우 새로운 레코드로 삽입, 존재하는 경우 업데이트
    query = """
        INSERT INTO kr_sector (IDX_CD, CMP_CD, CMP_KOR, SEC_NM_KOR, 기준일)
        VALUES (%s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
        IDX_CD = new.IDX_CD, CMP_KOR = new.CMP_KOR, SEC_NM_KOR = new.SEC_NM_KOR;
    """

    # 처리된 데이터 프레임에서 SQL 쿼리 실행을 위한 인자 리스트 추출
    args = kr_sector.values.tolist()

    # SQL 쿼리 실행
    mycursor.executemany(query, args)

    # 데이터베이스에 변경 사항 커밋
    con.commit()

    # 데이터베이스 연결 종료
    con.close()



def upsert_kr_code():
    """
    국내 주식시장 종목코드 및 표준코드 정보를 크롤링하고, 이를 클린징 처리하여 MySQL 데이터베이스의 kr_code 테이블에 정보를 삽입하거나 업데이트합니다.
    
    """
    # 데이터베이스 연결 생성
    con, mycursor = create_db_connection(db='stock')

    # 종목코드 데이터 크롤링
    data = crawl_code_data()

    # 종목코드 데이터 처리
    kr_code = process_code_data(data)

    # SQL 쿼리 설정: 존재하지 않는 경우 삽입, 이미 존재하는 경우 업데이트 (업서트 방식)
    query = """
        INSERT INTO kr_code (표준코드, 종목코드)
        VALUES (%s, %s) AS new
        ON DUPLICATE KEY UPDATE
        표준코드=new.표준코드, 종목코드=new.종목코드;
    """

    # 처리된 데이터 프레임에서 SQL 쿼리 실행을 위한 인자 리스트 추출
    args = kr_code.values.tolist()

    # SQL 쿼리 실행
    mycursor.executemany(query, args)

    # 데이터베이스에 변경 사항 커밋
    con.commit()

    # 데이터베이스 연결 종료
    con.close()



def upsert_kr_price():
    """
    주가 데이터를 MySQL 데이터베이스에 있는 kr_price 테이블에 정보를 삽입하거나 업데이트합니다
    
    반환: 
        error_list (list): 오류난 지점의 종목코드를 저장한 리스트입니다.
    """
    
    # 연결 객체와 커서 객체 생성하기
    engine = create_db_engine(db='stock')
    con, cursor = create_db_connection(db = 'stock')

    # 최신 기본정보 + 코드정보 불러오기
    base_df = fetch_latest_base(engine)
    code_list = fetch_kr_code(engine)
    
    # DB 저장 쿼리 
    query = """
        INSERT INTO kr_price (날짜, 시가, 고가, 저가, 종가, 거래량, 종목코드)
        VALUES (%s, %s, %s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
        시가 = new.시가, 고가 = new.고가, 저가 = new.저가, 종가 = new.종가, 거래량 = new.거래량;
    """
    
    # 오류 발생시 저장할 리스트 생성
    error_list = []
    
    # 종목 정보 병합
    merged_df = pd.merge(code_list, base_df, on='종목코드')
    
    # 전종목 주가 다운로드 및 저장
    for i in tqdm(range(len(merged_df))):
        try: 
            # 필요한 정보 추출
            CD_finder = merged_df.loc[i, '종목코드']
            STCD_finder = merged_df.loc[i, '표준코드']
            NM_finder = merged_df.loc[i, '종목명']

            output_data = crawl_price_data(CD_finder, STCD_finder, NM_finder)
            
            # 데이터 처리 및 DB 저장
            kr_price = process_price_data(output_data, CD_finder)
            args = kr_price.values.tolist()
            cursor.executemany(query, args)
            con.commit()
            
        except Exception as e:
            print(f"Error with {CD_finder}: {e}")
            error_list.append(CD_finder)
        
        time.sleep(2)  # Avoid rate limiting

    # Cleanup
    cursor.close()
    con.close()
    
    return error_list




def upsert_kr_fs():
    """
    재무 데이터를 MySQL 데이터베이스에 있는 kr_fs 테이블에 정보를 삽입하거나 업데이트합니다

    반환: 
        error_list (list): 오류난 지점의 종목코드를 저장한 리스트입니다.
    """

    # 연결 객체와 커서 객체 생성하기
    engine = create_db_engine(db='stock')
    con, cursor = create_db_connection(db = 'stock')

    # 기본정보 불러오기
    base_df = fetch_latest_base(engine)

    # DB 저장 쿼리
    query = """
        INSERT INTO kr_fs (계정, 기준일, 값, 종목코드, 공시구분)
        VALUES (%s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
        값 = new.값;
    """

    # 오류 발생시 저장할 리스트 생성
    error_list = []

    # 종목 리스트를 반복하며 재무 데이터 처리
    for i in tqdm(range(len(base_df))):
        try:
            # 티커 선택
            code = base_df.iloc[i]['종목코드']
            
            # 재무 데이터 크롤링
            data_fs_y, data_fs_q = crawl_financial_data(code)

            # 데이터 클렌징
            data_fs_y_clean = process_financial_data(data_fs_y, code, 'y')
            data_fs_q_clean = process_financial_data(data_fs_q, code, 'q')

            # 연간 데이터와 분기 데이터 합치기
            data_fs_bind = pd.concat([data_fs_y_clean, data_fs_q_clean])

            # 재무제표 데이터를 DB에 저장
            args = data_fs_bind.values.tolist()
            cursor.executemany(query, args)
            con.commit()

        except Exception:
            # 오류 발생시 해당 종목코드 저장 후 다음 루프로 이동
            error_list.append(code)

        # 타임슬립 적용
        time.sleep(2)

    # DB 연결 종료
    engine.dispose()
    con.close()
    
    return error_list  # 오류가 발생한 종목코드 리스트 반환



def upsert_kr_value():
    """
    분기별 재무제표와 기본 정보를 데이터베이스에서 불러와 가치지표를 계산한 뒤, 
    이를 데이터베이스의 kr_value 테이블에 저장하는 함수입니다.

    반환:
        없음
    """
    # DB 연결
    engine = create_db_engine(db='stock')
    con, cursor = create_db_connection(db='stock')

    # 분기 재무제표 불러오기
    fs_df = fetch_quarterly_financials(engine)

    # 기본 정보 불러오기
    base_df = fetch_latest_base(engine)

    # 가치지표 계산
    kr_value = calculate_value_indicators(fs_df, base_df)

    # 계산된 가치지표를 데이터베이스에 저장
    query = """
        INSERT INTO kr_value (종목코드, 기준일, 지표, 값)
        VALUES (%s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
        값 = new.값;
    """

    args_value = kr_value.values.tolist()
    cursor.executemany(query, args_value)
    con.commit()

    # DB 연결 종료
    engine.dispose()
    con.close()