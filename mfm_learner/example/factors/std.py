import logging

import numpy as np

from mfm_learner.datasource import datasource_utils
from mfm_learner.example.factors.factor import Factor
from mfm_learner.utils import utils

logger = logging.getLogger(__name__)


"""
波动率因子：
https://zhuanlan.zhihu.com/p/30158144
波动率因子有很多，我这里的是std，标准差，
而算标准差，又要设置时间窗口，这里设定了10，20，60，即半个月、1个月、3个月
"""

mapping = [
    {'name': 'std_10d', 'cname': '10日波动率', 'days': 10},
    {'name': 'std_1m', 'cname': '1月波动率', 'days': 20},
    {'name': 'std_3m', 'cname': '3月波动率', 'days': 60},
    {'name': 'std_6m', 'cname': '6月波动率', 'days': 120}
]


class StdFactor(Factor):

    def __init__(self):
        super().__init__()

    def name(self):
        return [m['name'] for m in mapping]

    def cname(self):
        return [m['cname'] for m in mapping]

    def calculate(self, stock_codes, start_date, end_date, df_daily=None):
        """
        计算波动率，波动率，就是往前回溯period个周期
        """
        results = []
        for m in mapping:
            start_days_go = utils.last_day(start_date, num=m['days'])
            df_daily = datasource_utils.load_daily_data(self.datasource, stock_codes, start_days_go, end_date)
            df_daily = datasource_utils.reset_index(df_daily)  # 设置日期+code为索引
            df_pct_chg = df_daily['pct_chg']
            df_std = df_pct_chg.rolling(window=m['days']).std()
            df_std.dropna(inplace=True)
            results.append(df_std)
        return results
