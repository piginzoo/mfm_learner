"""
参考：
https://zhuanlan.zhihu.com/p/161706770
"""
import logging

import numpy as np

from datasource import datasource_utils
from example.factors.factor import Factor

logger = logging.getLogger(__name__)

"""
# 规模因子 - 市值因子Market Value
"""


class MarketValueFactor(Factor):

    def __init__(self):
        super().__init__()

    def name(self):
        return "mv"


    def calculate(self, stock_codes, start_date, end_date):
        df_basic = self.datasource.daily_basic(stock_codes, start_date, end_date)
        df_basic = datasource_utils.reset_index(df_basic)
        df_basic['LNCAP'] = np.log(df_basic['total_mv'])
        logger.debug("计算完%s~%s市值因子(LNCAP)，股票[%r], %d 条因子值", start_date, end_date, stock_codes, len(df_basic))
        assert len(df_basic) > 0, df_basic
        return df_basic['LNCAP']
