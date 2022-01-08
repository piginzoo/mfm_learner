"""
参考：
https://zhuanlan.zhihu.com/p/161706770
"""
import logging

import numpy as np

from datasource import datasource_utils
from example.factor import Factor

logger = logging.getLogger(__name__)

"""
# 规模因子 - 市值因子Market Value
"""


class MarketValueFactor(Factor):

    def __init__(self):
        super().__init__()

    def calculate(self, stock_codes, start_date, end_date, df_daily=None):
        if df_daily is None:
            df_daily = datasource_utils.load_daily_data(self.datasource, stock_codes, start_date, end_date)

        df_daily['LNCAP'] = np.log(df_daily['total_mv'])
        logger.debug("计算完市值因子(LNCAP)，%d 条因子值", len(df_daily))

        return df_daily['LNCAP']
