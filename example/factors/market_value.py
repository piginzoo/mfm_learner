"""
参考：
https://zhuanlan.zhihu.com/p/161706770
"""
import logging

import numpy as np
import pandas as pd

import tushare_utils

logger = logging.getLogger(__name__)

"""
# 规模因子 - 市值因子Market Value
"""

def load_stock_data(stock_codes, start, end):
    df_merge = None
    for stock_code in stock_codes:
        df_basic = tushare_utils.daily_basic(stock_code=stock_code, start_date=start, end_date=end)
        if df_merge is None:
            df_merge = df_basic
        else:
            df_merge = df_merge.append(df_basic)
        logger.debug("加载%s~%s的股票[%s]的%d条交易和基本信息的合并数据", start, end, stock_code, len(df_merge))
    logger.debug("一共加载%s~%s %d条数据", start, end, len(df_merge))
    return df_merge


def get_factor(stock_codes, start, end):
    """
    市值因子
    :param universe:
    :param start:
    :param end:
    :param file_name:
    :return:
    """
    stock_data = load_stock_data(stock_codes, start=start, end=end)
    factors = stock_data
    factors['LNCAP'] = np.log(stock_data['total_mv'])
    factors = factors[['trade_date', 'ts_code', 'LNCAP']]
    factors['trade_date'] = pd.to_datetime(factors['trade_date'], format="%Y%m%d")  # 时间为日期格式，tushare是str
    factors = factors.set_index(['trade_date', 'ts_code'])
    logger.debug("计算完市值因子(LNCAP)，%d 条因子值", len(factors))
    return factors
