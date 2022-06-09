"""
用来生成mock数据
"""
from functools import reduce
from random import random

import pandas as pd
from pandas import DataFrame

from mfm_learner.datasource import datasource_factory, datasource_utils
from mfm_learner.utils import utils

# pytest test/unitest/test_utils.py -s


def generate_mock_trade_data():
    data1 = [
        ['000001.SZ', '2016-06-24', 0.165260, 0.002198, 0.085632, -0.078074, 0.173832, 0.214377, 0.068445],
        ['000001.SZ', '2016-06-27', 0.165537, 0.003583, 0.063299, -0.048674, 0.180890, 0.202724, 0.081748],
        ['000001.SZ', '2016-06-28', 0.135215, 0.010403, 0.059038, -0.034879, 0.111691, 0.122554, 0.042489],
        ['000002.SH', '2016-06-24', 0.068774, 0.019848, 0.058476, -0.049971, 0.042805, 0.053339, 0.079592],
        ['000002.SH', '2016-06-27', 0.039431, 0.012271, 0.037432, -0.027272, 0.010902, 0.077293, -0.050667],
        ['000002.SH', '2016-06-28', 0.039431, 0.012271, 0.037432, -0.027272, 0.010902, 0.077293, -0.050667],
        ['000002.SH', '2016-06-29', 0.039431, 0.012271, 0.037432, -0.027272, 0.010902, 0.077293, -0.050667]
    ]
    data1 = pd.DataFrame(data1,columns=["code", "datetime", "BP", "CFP", "EP", "ILLIQUIDITY", "REVS20", "SRMI", "VOL20"])
    data1['datetime'] = pd.to_datetime(data1['datetime'], format='%Y-%m-%d')  # 时间为日期格式，tushare是str
    data1 = data1.set_index(["datetime", "code"])
    return data1


def generate_factor_data(start_date='20180101',end_date='20181001',stock_num=10):
    # 获得交易日期
    dates = datasource_factory.get().trade_cal(start_date, end_date)
    # 使用中证500股票池
    index_code = "000905.SH"
    # 挑出一些股票
    stocks = datasource_factory.get().index_weight(index_code, start_date)[:stock_num]
    factor_value = __generate_mock_column(stocks,dates,'factor')
    return_1D = __generate_mock_column(stocks,dates,'1D')
    return_5D = __generate_mock_column(stocks, dates, '5D')
    return_10D = __generate_mock_column(stocks, dates, '10D')
    data_frames = [factor_value,return_1D,return_5D,return_10D]
    df_merged = reduce(lambda left, right: pd.merge(left, right,left_index=True, right_index=True),
                       data_frames)
    print(df_merged.head(5))
    return df_merged

def __generate_mock_column(stocks, dates, column_name):
    df = DataFrame()
    for d in dates:
        for s in stocks:
            df = df.append([[d, s, random()]])
    df.columns = ['date', 'asset', column_name] # 这个列名是alphalens要求的
    df['date'] = datasource_utils.to_datetime(df['date'])
    df = df.set_index(['date', 'asset'])
    return df

def test_get_monthly_duration():
    scopes = utils.get_monthly_duration("20180312", "20220515")
    assert scopes[0][0] == '20180312'
    assert scopes[0][1] == '20180331'
    assert scopes[1][0] == '20180401'
    assert scopes[1][1] == '20180430'
    assert scopes[-1][0] == '20220501'
    assert scopes[-1][1] == '20220515'
    assert len(scopes)==(10+12+12+12+5) # 2018,2019,2020,2021,2022