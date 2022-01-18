# pytest  test/test_factor_utils.py -s
import math
from random import random

import numpy as np
import pandas as pd
from pandas import DataFrame

from datasource import datasource_factory
from example import factor_utils

allowed_error = 0.00000001


def test_pct_chg():
    data = pd.Series([1, 1.1, 1.21, 1.331])
    returns = factor_utils.pct_chg(data, 1)
    assert abs(returns[0] - 0.1) < allowed_error
    assert abs(returns[1] - 0.1) < allowed_error
    assert abs(returns[2] - 0.1) < allowed_error
    assert math.isnan(returns[3])


"""
def neutralize(factor_df,
               group,
               float_mv=None,
               index_member=None):
"""


def test_neutralize():
    df_factor = __generate_mock_factor()
    factor_utils.neutralize(df_factor)


def __generate_mock_factor():
    start_date = '20200101'
    end_date = '20201201'

    dates = datasource_factory.get().trade_cal(start_date, end_date)

    stocks = datasource_factory.get().index_weight('000300.SH', start_date)
    np.random.shuffle(stocks)
    stocks = stocks[:5]

    df = DataFrame()
    for d in dates:
        for s in stocks:
            df = df.append([[d, s, random()]])
    df.columns = ['date', 'code', 'value']
    return df
