import pandas as pd
import numpy as np
from scipy.stats import zscore

def process_market_data(data, mkt_day):
    """
    시장 데이터 프레임과 개별종목 데이터 프레임을 합치고 클린징 작업을 수행하는 함수입니다.

    매개변수:
        data (list): 코스피와 코스닥 시장 그리고 개별종목 데이터 프레임의 리스트입니다.
        mkt_day (str): 'YYYYMMDD' 형식의 시장 거래일입니다.

    반환:
        kr_base DataFrame: 클린징 작업이 완료된 합쳐진 데이터 프레임입니다.
    """
    # 두 개의 데이터 프레임 합치기
    kr_mkt = pd.concat([data[0], data[1]], ignore_index=True)

    # 클린징 작업: 종목명에서 공백 제거
    kr_mkt['종목명'] = kr_mkt['종목명'].str.strip()

    # 기준일 컬럼 추가
    kr_mkt['기준일'] = mkt_day 

    kr_idv = data[2]
    kr_idv['종목명'] = kr_idv['종목명'].str.strip()
    kr_idv['기준일'] = mkt_day 

    # 두 데이터프레임에 공통으로 존재하지 않는 종목 찾기
    diff = list(set(kr_mkt['종목명']).symmetric_difference(set(kr_idv['종목명'])))

    # 2.6 두 데이터프레임 합치기
    kr_base = pd.merge(kr_mkt,
                        kr_idv,
                        on = kr_mkt.columns.intersection(
                            kr_idv.columns).tolist(),
                        how = 'outer')

    # 종목구분 컬럼추가 (일반적인 종목과 스팩, 우선주, 리츠, 기타 주식 구분)
    kr_base['종목구분']= np.where(kr_base['종목명'].str.contains('스팩|제[0-9]+호'), '스팩', 
                                np.where(kr_base['종목코드'].str[-1:] != '0', '우선주', 
                                        np.where(kr_base['종목명'].str.endswith('리츠'), '리츠', 
                                                np.where(kr_base['종목명'].isin(diff), '기타', '보통주'))))

    kr_base = kr_base.reset_index(drop=True)
    # 열 이름에 있는 공백 없애기
    kr_base.columns = kr_base.columns.str.replace(' ', '')

    # 필요한 열만 선택
    kr_base = kr_base[['종목코드', '종목명', '시장구분', '종가', '시가총액', 
                            '기준일', 'EPS', '선행EPS', 'BPS', '주당배당금', '종목구분']]
    
    # nan은 SQL에 저장하지 못하므로 None으로 변경
    kr_base = kr_base.replace({np.nan: None})

    # 최종 클린징된 데이터 프레임 반환
    return kr_base

def process_sector_data(data, mkt_day):
    """
    리스트에 있는 각 섹터 데이터프레임들을 합치고 클린징 작업을 수행하는 함수입니다.

    매개변수:
        data (list): 크롤링으로 얻은 섹터 데이터프레임들이 담긴 리스트입니다.
        mkt_day (str): 'YYYYMMDD' 형식의 시장 거래일입니다.

    반환:
        kr_sector (DataFrame): 클린징 작업이 완료된 합쳐진 데이터 프레임입니다.
    """
    # 리스트에 있는 데이터프레임들 합치기
    kr_sector = pd.concat(data, axis=0)

    # 필요한 컬럼만 선택
    kr_sector = kr_sector[['IDX_CD', 'CMP_CD', 'CMP_KOR', 'SEC_NM_KOR']]

    # 기준일 컬럼 추가
    kr_sector['기준일'] = mkt_day

    # 기준일 데이터타임 형태로 변경
    kr_sector['기준일'] = pd.to_datetime(kr_sector['기준일'], format='%Y%m%d')

    return kr_sector

def process_code_data(data):
    """
    리스트 형태의 코드 데이터를 받아 Dataframe으로 변환시키고 클린징 처리하는 함수입니다.

    매개변수:
        data (list): 크롤링으로 얻은 종목코드 데이터가 담긴 리스트입니다.

    반환:
        kr_code (DataFrame): '표준코드'와 '종목코드'만을 포함하는 DataFrame입니다.
    """
    # 리스트에서 DataFrame 생성
    data_code = pd.DataFrame(data)
    
    # 필요한 컬럼만 선택
    kr_code = data_code.iloc[:, 0:2]
    
    # 컬럼 이름 변경
    kr_code.columns = ['표준코드', '종목코드']

    return kr_code

def process_price_data(data, CD_finder):
    """
    리스트 형태의 주식 가격 데이터를 받아 Dataframe으로 변환시키고 클린징 처리하는 함수입니다.

    매개변수:
        data (list): 주식 가격 데이터가 담긴 리스트입니다.
        CD_finder (str): 주식 종목 코드입니다.

    반환:
        kr_price (DataFrame): 처리된 주식 가격 정보가 담긴 데이터 프레임입니다.
    """
    # 리스트에서 DataFrame 생성
    data_price = pd.DataFrame(data)
    
    # 필요한 컬럼만 선택 및 컬럼 이름 변경
    kr_price = data_price[['TRD_DD', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC', 'ACC_TRDVOL']]
    kr_price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
    
    # 결측치 제거
    kr_price = kr_price.dropna()
    
    # 컬럼의 쉼표 제거 및 데이터 타입 변경
    column_obj = ['시가', '고가', '저가', '종가', '거래량']
    for column in column_obj:
        kr_price[column] = kr_price[column].str.replace(',', '').astype(float)
    
    # '날짜' 컬럼을 datetime 타입으로 변경
    kr_price['날짜'] = pd.to_datetime(kr_price['날짜'])
    
    # '종목코드' 컬럼 추가
    kr_price['종목코드'] = CD_finder
    
    return kr_price

def process_financial_data(data, code, frequency):

    data = data[~data.loc[:, ~data.columns.isin(['계정'])].isna().all(axis=1)]
    data = data.drop_duplicates(['계정'], keep='first')
    data = pd.melt(data, id_vars='계정', var_name='기준일', value_name='값')
    data = data[~pd.isnull(data['값'])]
    data['계정'] = data['계정'].replace({'계산에 참여한 계정 펼치기': ''}, regex=True)
    data['기준일'] = pd.to_datetime(data['기준일'],
                               format='%Y/%m') + pd.tseries.offsets.MonthEnd()
    data['종목코드'] = code
    data['공시구분'] = frequency

    return data


def calculate_value_indicators(fs_df, base_df):
    """
    재무 데이터와 기본 정보를 기반으로 다양한 가치지표를 계산하는 함수입니다.

    매개변수:
        fs_df (DataFrame): 정렬된 재무 데이터가 담긴 데이터 프레임입니다.
        base_df (DataFrame): 시가총액 및 기타 정보가 담긴 기본 정보 데이터 프레임입니다.

    반환:
        kr_value (DataFrame): 계산된 가치지표가 담긴 데이터 프레임입니다.
    """
    # 재무 데이터 정렬
    fs_df = fs_df.sort_values(['종목코드', '계정', '기준일'])
    
    # TTM(지난 4분기 합계) 계산
    fs_df['ttm'] = fs_df.groupby(['종목코드', '계정'], as_index=False)['값'].rolling(window=4, min_periods=4).sum()['값']
    
    # 자본의 경우 TTM을 평균으로 계산
    fs_df['ttm'] = np.where(fs_df['계정'] == '자본', fs_df['ttm'] / 4, fs_df['ttm'])
    
    # 최신 분기만 선택
    fs_df = fs_df.groupby(['계정', '종목코드']).tail(1)
    
    # 가치지표 계산
    fs_df_merge = fs_df[['계정', '종목코드', 'ttm']].merge(base_df[['종목코드', '시가총액', '기준일']], on='종목코드')
    fs_df_merge['시가총액'] = fs_df_merge['시가총액'] / 100000000  # 시가총액을 1억 원 단위로 변경
    fs_df_merge['value'] = fs_df_merge['시가총액'] / fs_df_merge['ttm']
    fs_df_merge['value'] = fs_df_merge['value'].round(4)
    
    # 지표명 설정
    fs_df_merge['지표'] = np.where(
        fs_df_merge['계정'] == '매출액', 'PSR',
        np.where(
            fs_df_merge['계정'] == '영업활동으로인한현금흐름', 'PCR',
            np.where(
                fs_df_merge['계정'] == '자본', 'PBR',
                np.where(fs_df_merge['계정'] == '당기순이익', 'PER', None)
            )
        )
    )
    
    # 컬럼명 변경 및 필요한 컬럼 선택
    fs_df_merge.rename(columns={'value': '값'}, inplace=True)
    fs_df_merge = fs_df_merge[['종목코드', '기준일', '지표', '값']]
    fs_df_merge = fs_df_merge.replace([np.inf, -np.inf, np.nan], None)  # 무한대 및 NaN 처리
    
    # 배당수익률 계산 및 클린징
    base_df['값'] = base_df['주당배당금'] / base_df['종가']
    base_df['값'] = base_df['값'].round(4)
    base_df['지표'] = 'DY'
    value_dy = base_df[['종목코드', '기준일', '지표', '값']]
    value_dy = value_dy.replace([np.inf, -np.inf, np.nan], None)
    value_dy = value_dy[value_dy['값'] != 0]
    
    # 재무지표와 배당수익률 데이터 합치기
    kr_value = pd.concat([fs_df_merge, value_dy]).reset_index(drop=True)
    
    return kr_value



def to_zscore(df, cutoff=0.01, asc=False):
    """
    데이터 프레임의 모든 수치를 Z-score로 변환합니다. 이 때, 양쪽 끝에서 특정 비율(cutoff)만큼을 제외한 후 계산합니다.

    매개변수:
        df (DataFrame): Z-score로 변환할 데이터 프레임입니다.
        cutoff (float): 양 끝에서 제외할 데이터의 비율입니다. 기본값은 0.01 입니다.
        asc (bool): 순위의 오름차순 여부입니다. False일 경우 내림차순으로 순위를 매깁니다. 기본값은 False입니다.

    반환:
        df_z_score (DataFrame): Z-score로 변환된 데이터 프레임입니다.
    """
    # 데이터 프레임의 하위 및 상위 cutoff에 해당하는 값을 구함
    q_low = df.quantile(cutoff)
    q_hi = df.quantile(1 - cutoff)

    # cutoff 기준으로 데이터를 필터링
    df_trim = df[(df > q_low) & (df < q_hi)]

    # asc 매개변수에 따라 데이터의 순위를 매긴 후 Z-score로 변환
    # 내림차순일 경우
    if asc == False:
        df_z_score = df_trim.rank(axis=0, ascending=False).apply(
            zscore, nan_policy='omit')
    # 오름차순일 경우
    if asc == True:
        df_z_score = df_trim.rank(axis=0, ascending=True).apply(
            zscore, nan_policy='omit')

    return df_z_score
