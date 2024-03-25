from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy.stats import zscore

from database.mysql_reader import create_db_engine, fetch_latest_base, fetch_quarterly_financials_ver2, fetch_latest_value, fetch_recent_year_price, fetch_latest_sector
from data.cleanser import to_zscore

def model_portfolio():
    """
    이 함수는 주식 포트폴리오 모델링을 위해 필요한 데이터를 불러오고, 
    이를 기반으로 한 투자 결정 과정을 진행합니다.

    반환:
        final_df (Dataframe): 이 함수의 최종 결과물로, 각 종목에 대한 종합적인 투자 지표와 투자 결정을 포함한 데이터프레임
    """

    # 데이터베이스 엔진 생성
    engine = create_db_engine(db='stock')
    # 필요한 데이터 불러오기
    base_df = fetch_latest_base(engine)
    fs_df = fetch_quarterly_financials_ver2(engine)
    value_df = fetch_latest_value(engine)
    price_df = fetch_recent_year_price(engine)
    sector_df = fetch_latest_sector(engine)
    engine.dispose()  # 데이터베이스 연결 해제

    # TTM 기준으로 퀄리티 지표 계산
    fs_df = fs_df.sort_values(['종목코드', '계정', '기준일'])
    fs_df['ttm'] = fs_df.groupby(['종목코드', '계정'], as_index=False)['값'].rolling(window=4, min_periods=4).sum()['값']
    fs_df_clean = fs_df.copy()
    fs_df_clean['ttm'] = np.where(fs_df_clean['계정'].isin(['자산', '자본']),
                                  fs_df_clean['ttm'] / 4, fs_df_clean['ttm'])
    fs_df_clean = fs_df_clean.groupby(['종목코드', '계정']).tail(1)
    fs_df_pivot = fs_df_clean.pivot(index='종목코드', columns='계정', values='ttm')
    fs_df_pivot['ROE'] = fs_df_pivot['당기순이익'] / fs_df_pivot['자본']
    fs_df_pivot['GPA'] = fs_df_pivot['매출총이익'] / fs_df_pivot['자산']
    fs_df_pivot['CFO'] = fs_df_pivot['영업활동으로인한현금흐름'] / fs_df_pivot['자산']

    # 음수인 가치지표 처리
    value_df.loc[value_df['값'] <= 0, '값'] = np.nan
    value_df_pivot = value_df.pivot(index='종목코드', columns='지표', values='값')

    # 최근 12개월 수익률 및 K-Ratio 계산
    price_pivot = price_df.pivot(index='날짜', columns='종목코드', values='종가')
    ret_list = pd.DataFrame(data=(price_pivot.iloc[-1] / price_pivot.iloc[0]) - 1, columns=['12M'])
    ret = price_pivot.pct_change().iloc[1:]
    ret_cum = np.log(1 + ret).cumsum()

    x = np.array(range(len(ret)))
    k_ratio = {}
    for i in range(len(base_df)):
        code = base_df.loc[i, '종목코드']
        try:
            y = ret_cum.loc[:, price_pivot.columns == code]
            reg = sm.OLS(y, x).fit()
            res = float(reg.params / reg.bse)
        except:
            res = np.nan
        k_ratio[code] = res
    k_ratio_df = pd.DataFrame.from_dict(k_ratio, orient='index').reset_index()
    k_ratio_df.columns = ['종목코드', 'K_ratio']


    # 데이터 병합 및 섹터 정보 처리
    combined_df = base_df[['종목코드', '종목명']].merge(sector_df[['CMP_CD', 'SEC_NM_KOR']], how='left', left_on='종목코드', right_on='CMP_CD')
    combined_df = combined_df.merge(fs_df_pivot[['ROE', 'GPA', 'CFO']], how='left', on='종목코드')
    combined_df = combined_df.merge(value_df_pivot, how='left', on='종목코드')
    combined_df = combined_df.merge(ret_list, how='left', on='종목코드')
    combined_df = combined_df.merge(k_ratio_df, how='left', on='종목코드') # 모든 테이블의 병합
    combined_df.loc[combined_df['SEC_NM_KOR'].isnull(), 'SEC_NM_KOR'] = '기타'
    combined_df = combined_df.drop(['CMP_CD'], axis=1)

    # 종목코드와 섹터정보(SEC_NM_KOR)를 인덱스로 설정하는, 섹터에 따른 그룹 데이터프레임 생성
    combined_df_group = combined_df.set_index(['종목코드', 'SEC_NM_KOR']).groupby('SEC_NM_KOR', as_index=False)

    # 각 팩터에 해당하는 z-score로 정규화한 후 합산, 새로 이름짓고 데이터 프레임 항목에 추가
    # 퀄리티 지표(z_quality)
    z_quality = combined_df_group[['ROE', 'GPA', 'CFO']].apply(lambda x: to_zscore(x, 0.01, False)).sum(axis=1, skipna=False).to_frame('z_quality')
    combined_df = combined_df.merge(z_quality, how='left', on=['종목코드', 'SEC_NM_KOR'])

    # 밸류 지표(z_value)
    value_Ps = combined_df_group[['PBR', 'PCR', 'PER','PSR']].apply(lambda x: to_zscore(x, 0.01, True))
    value_DY = combined_df_group[['DY']].apply(lambda x: to_zscore(x, 0.01, False))
    z_value = value_Ps.merge(value_DY, on=['종목코드', 'SEC_NM_KOR']).sum(axis=1,skipna=False).to_frame('z_value')
    combined_df = combined_df.merge(z_value, how='left', on=['종목코드', 'SEC_NM_KOR'])

    # 모멘텀 지표(z_momentum)
    z_momentum = combined_df_group[['12M', 'K_ratio']].apply(lambda x: to_zscore(x, 0.01, False)).sum(axis=1, skipna=False).to_frame('z_momentum')
    combined_df = combined_df.merge(z_momentum, how='left', on=['종목코드', 'SEC_NM_KOR'])

    # 추가 계산을 위하여 z_quality, z_value, z_momentum을 하나로 묶은 데이터 프레임을 만든다
    qvm_df = combined_df[['종목코드', 'z_quality', 'z_value', 'z_momentum']].set_index('종목코드').apply(zscore,nan_policy='omit')
    qvm_df.columns = ['quality', 'value', 'momentum']

    wts = [0.3, 0.3, 0.3] # 각 팩터별 비중 리스트
    qvm_df_sum = (qvm_df * wts).sum(axis=1, skipna=False).to_frame() # 팩터별 z-score의 합산
    qvm_df_sum.columns = ['qvm'] 
    final_df = combined_df.merge(qvm_df_sum, on='종목코드') # 합산 값을 기존 데이터 프레임에 추가
    final_df['invest'] = np.where(final_df['qvm'].rank() <= 20, 'Y', 'N') # 합산 값 상위 20위에 대한 투자값을 'Y'(YES), 그 외를 'N'(NO)로 입력한다
    return final_df

