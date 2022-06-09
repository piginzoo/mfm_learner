"""
参考：
https://zhuanlan.zhihu.com/p/161706770
"""
import logging

import numpy as np

from mfm_learner.datasource import datasource_utils
from mfm_learner.example.factors.factor import Factor

logger = logging.getLogger(__name__)

"""
# 规模因子 - 市值因子Market Value
"""


class MarketValueFactor(Factor):
    """
    市值因子LNAP，是公司股票市值的自然对数，
    """

    def __init__(self):
        super().__init__()

    def name(self):
        return "mv"

    def cname(self):
        return "市值"

    def calculate(self, stock_codes, start_date, end_date):
        df_basic = self.datasource.daily_basic(stock_codes, start_date, end_date)
        df_basic = datasource_utils.reset_index(df_basic)
        df_basic['LNAP'] = np.log(df_basic['total_mv'])
        logger.debug("计算完%s~%s市值因子(LNAP)，股票[%r], %d 条因子值", start_date, end_date, stock_codes, len(df_basic))
        assert len(df_basic) > 0, df_basic
        return df_basic['LNAP']


class CirculationMarketValueFactor(Factor):
    """
    流动市值因子LNCAP，是公司股票流通市值的自然对数
    """

    def __init__(self):
        super().__init__()

    def name(self):
        return "cmv"

    def cname(self):
        return "流通市值"

    def calculate(self, stock_codes, start_date, end_date):
        df_basic = self.datasource.daily_basic(stock_codes, start_date, end_date)
        df_basic = datasource_utils.reset_index(df_basic)
        df_basic['circ_mv'] = np.log(df_basic['circ_mv']) # 做个log处理，否则相差太大
        logger.debug("计算完%s~%s流通市值因子，股票[%r], %d 条因子值", start_date, end_date, stock_codes, len(df_basic))
        assert len(df_basic) > 0, df_basic
        return df_basic['circ_mv']
