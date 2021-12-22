"""
参考：
https://zhuanlan.zhihu.com/p/161706770
"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def get_factor(stock_data):
    """
    市值因子
    :param universe:
    :param start:
    :param end:
    :param file_name:
    :return:
    """
    factors = stock_data.copy()
    factors['LNCAP'] = np.log(stock_data['total_mv'])
    factors = factors[['trade_date', 'ts_code', 'LNCAP']]
    factors['trade_date'] = pd.to_datetime(factors['trade_date'], format="%Y%m%d")  # 时间为日期格式，tushare是str
    factors = factors.set_index(['trade_date', 'ts_code'])
    logger.debug("计算完市值因子(LNCAP)，%d 条因子值", len(factors))
    return factors
