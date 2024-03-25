import os
import pandas as pd
from sqlalchemy import create_engine

def create_db_engine(db):
    """
    데이터베이스 연결 엔진을 생성하여 반환합니다.
    
    매개변수:
        db (str): 연결할 데이터베이스의 이름입니다. 'stock'과 같은 문자열을 예상합니다.
    반환:
        engine: SQLAlchemy를 통해 생성된 데이터베이스 연결 엔진입니다.
    """
    # 환경변수에서 데이터베이스 비밀번호를 가져오기
    pswd = os.getenv('pswd2')
    
    # SQLAlchemy를 사용한 데이터베이스 연결 엔진 생성
    engine = create_engine(f'mysql+pymysql://root:{pswd}@127.0.0.1:3306/{db}')
    
    return engine


def fetch_latest_base(engine):
    """
    데이터베이스에서 가장 최근일에 해당하는 보통주의 기본 정보를 가져옵니다.

    매개변수:
        engine: 데이터베이스 연결 엔진 객체입니다.

    반환:
        base_df (DataFrame): 가장 최근일에 해당하는 보통주 기본 정보가 담긴 데이터 프레임입니다.
    """
    # 가장 최근일의 보통주 티커 정보 조회
    base_df = pd.read_sql("""
        SELECT * FROM kr_base
        WHERE 기준일 = (SELECT MAX(기준일) FROM kr_base)
        AND 종목구분 = '보통주';
    """, con=engine)

    return base_df

def fetch_kr_code(engine):
    """
    데이터베이스에서 'kr_code' 테이블의 모든 내용을 가져옵니다.

    매개변수:
        engine: 데이터베이스 연결 엔진 객체입니다.

    반환:
        code_df (DataFrame): 표준코드와 종목코드 내용이 담긴 데이터 프레임입니다.
    """
    # 'kr_code' 테이블의 내용 조회
    code_df = pd.read_sql("SELECT * FROM kr_code", con=engine)

    return code_df

def fetch_quarterly_financials(engine):
    """
    데이터베이스에서 '당기순이익', '자본', '영업활동으로인한현금흐름', '매출액'에 해당하는 분기별 재무 데이터를 가져옵니다.

    매개변수:
        engine: SQLAlchemy 엔진 객체입니다. 데이터베이스와의 연결을 위해 사용됩니다.

    반환:
        fs_df (DataFrame): 분기별 재무 데이터를 포함하는 데이터 프레임입니다.
    """
    # 분기별 재무 데이터 조회 쿼리 실행
    fs_df = pd.read_sql("""
    SELECT * FROM kr_fs
    WHERE 공시구분 = 'q'
    AND 계정 IN ('당기순이익', '자본', '영업활동으로인한현금흐름', '매출액');
    """, con=engine)

    return fs_df


import pandas as pd

def fetch_quarterly_financials_ver2(engine):
    """
    데이터베이스에서 '당기순이익', '매출총이익', '영업활동으로인한현금흐름', '자산', '자본' 항목을 포함하는 분기별 재무 데이터를 가져옵니다.

    매개변수:
        engine: 데이터베이스 연결 엔진입니다.

    반환:
        fs_df (DataFrame): 분기별 재무 데이터를 포함하는 데이터 프레임입니다.
    """
    query = """
        SELECT * FROM kr_fs
        WHERE 계정 IN ('당기순이익', '매출총이익', '영업활동으로인한현금흐름', '자산', '자본')
        AND 공시구분 = 'q';
    """
    fs_df = pd.read_sql(query, con=engine)
    return fs_df



def fetch_latest_value(engine):
    """
    데이터베이스에서 가장 최근 기준일에 해당하는 value_df 테이블의 정보를 가져오는 함수입니다.

    매개변수:
        engine: 데이터베이스 연결을 위한 SQLAlchemy 엔진 객체입니다.

    반환:
        value_df (DataFrame): 'value_df' 테이블에서 가장 최근 기준일의 정보를 담은 데이터 프레임입니다.
    """
    # 가장 최근 기준일의 데이터 조회 쿼리 실행
    value_df = pd.read_sql("""
    SELECT * FROM kr_value
    WHERE 기준일 = (SELECT MAX(기준일) FROM kr_value);
    """, con=engine)

    return value_df

def fetch_recent_year_price(engine):
    """
    데이터베이스에서 최근 1년 간의 주가 정보를 가져오는 함수입니다.

    매개변수:
        engine: 데이터베이스 연결을 위한 SQLAlchemy 엔진 객체입니다.

    반환:
        price_df (DataFrame): 최근 1년 간의 날짜, 종가, 종목코드를 포함하는 데이터 프레임입니다.
    """
    # 최근 1년 간의 주가 정보 조회 쿼리 실행
    price_df = pd.read_sql("""
    SELECT 날짜, 종가, 종목코드
    FROM kr_price
    WHERE 날짜 >= (SELECT MAX(날짜) FROM kr_price) - INTERVAL 1 YEAR;
    """, con=engine)

    return price_df

def fetch_latest_sector(engine):
    """
    데이터베이스에서 가장 최근 기준일에 해당하는 섹터 정보를 가져오는 함수입니다.

    매개변수:
        engine: 데이터베이스 연결을 위한 SQLAlchemy 엔진 객체입니다.

    반환:
        sector_df (DataFrame): 'kr_sector' 테이블에서 가장 최근 기준일의 섹터 정보를 담은 데이터 프레임입니다.
    """
    # 가장 최근 기준일의 섹터 정보 조회 쿼리 실행
    sector_df = pd.read_sql("""
    SELECT * FROM kr_sector
    WHERE 기준일 = (SELECT MAX(기준일) FROM kr_sector);
    """, con=engine)

    return sector_df