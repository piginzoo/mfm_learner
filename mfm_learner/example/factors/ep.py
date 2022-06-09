"""
盈利收益率
"""
import logging
import math

import numpy as np

from mfm_learner.datasource import datasource_utils
from mfm_learner.example.factors.factor import Factor

logger = logging.getLogger(__name__)

"""
盈利收益率 EP（Earn/Price） = 盈利/价格

其实，就是1/PE（市盈率），

这里，就说说PE，因为EP就是他的倒数：

PE = PRICE / EARNING PER SHARE，指股票的本益比，也称为“利润收益率”。 

本益比是某种股票普通股每股市价与每股盈利的比率，所以它也称为股价收益比率或市价盈利比率。

- [基本知识解读 -- PE, PB, ROE，盈利收益率](https://xueqiu.com/4522742712/61623733)

"""

class EPFactor(Factor):

    def __init__(self):
        super().__init__()

    def name(self):
        return "ep"

    def calculate(self, stock_codes, start_date, end_date, df_daily=None):
        df_basic = self.datasource.daily_basic(stock_codes, start_date, end_date)
        df_basic = datasource_utils.reset_index(df_basic)
        return 1 / df_basic['pe']  # pe是市赢率，盈利收益率 EP比是1/pe