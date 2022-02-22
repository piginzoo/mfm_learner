# pytest  test/unitest/test_factor_utils.py -s
import math
from random import random

import pandas as pd
from pandas import DataFrame

from utils import utils

utils.init_logger()

from datasource import datasource_factory, datasource_utils
from example import factor_utils

allowed_error = 0.00000001


def test_pct_chg():
    data = pd.Series([1, 1.1, 1.21, 1.331])
    returns = factor_utils.pct_chg(data, 1)
    assert abs(returns[0] - 0.1) < allowed_error
    assert abs(returns[1] - 0.1) < allowed_error
    assert abs(returns[2] - 0.1) < allowed_error
    assert math.isnan(returns[3])


def test_neutralize():
    """
    测试行业中性化
    def neutralize(factor_df,
                   group,
                   float_mv=None,
                   index_member=None):
    """
    start_date = '20180101'
    end_date = '20191201'
    stocks = datasource_factory.get().index_weight('000905.SH', start_date,end_date)
    # np.random.shuffle(stocks)
    assert len(stocks)>0, len(stocks)
    stocks = stocks[:5]

    # 行业数据
    df_factor = __generate_mock_factor(stocks, start_date, end_date)
    # df_industry = datasource_utils.compile_industry(df_industry)
    # # 市值数据
    # df_mv = datasource_factory.get().daily_basic(stocks, start_date, end_date)
    # df_mv = datasource_utils.reset_index(df_mv)
    neutralized_factor = factor_utils.neutralize(df_factor)  # df_industry, df_mv['total_mv'])

    print("中性化结果：")
    print(neutralized_factor)


def __generate_mock_factor(stocks, start_date, end_date):
    """
    因子造假器，哈哈哈
    :return:
    """
    # 获得交易日期
    dates = datasource_factory.get().trade_cal(start_date, end_date)

    # stocks_info = datasource_factory.get().stock_basic(",".join(stocks))

    df = DataFrame()
    for d in dates:
        for s in stocks:
            df = df.append([[d, s, random()]])
    df.columns = ['datetime', 'code', 'value']

    # df = df.merge(stocks_info[['code', 'industry']], on="code")

    print("因子：", df.head(3))

    df = datasource_utils.reset_index(df)

    return df


def test_handle_finance_ttm():
    start_date = '20180101'
    end_date = '20191201'
    stock_codes = ['600000.SH']  # 浦发银行

    # 为TTM，把时间提前2年
    start_date_2years_ago = utils.last_year(start_date, num=2)
    datasource = datasource_factory.get()
    trade_dates = datasource.trade_cal(start_date, end_date)

    df_finance = datasource.fina_indicator(stock_codes, start_date_2years_ago, end_date)

    df = factor_utils.handle_finance_ttm(stock_codes,
                                         df_finance,
                                         trade_dates,
                                         col_name_value='roe',
                                         col_name_finance_date='end_date')
    print("ROE_TTM:")
    print(df)