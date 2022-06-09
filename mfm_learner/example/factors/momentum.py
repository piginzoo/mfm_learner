import logging

import numpy as np

from mfm_learner.datasource import datasource_utils
from mfm_learner.example.factors.factor import Factor
from mfm_learner.utils import utils

logger = logging.getLogger(__name__)
period_window = 10

"""
动量因子：
    动量因子是指与股票的价格和交易量变化相关的因子，常见的动量因子：一个月动量、3个月动量等。
    计算个股（或其他投资标的）过去N个时间窗口的收益回报：
    adj_close = (high + low + close)/3
    adj_return = adj_close_t - adj_close_{t-n}
    来计算受盘中最高价和最低价的调整的调整收盘价动量，逻辑是，在日线的层面上收盘价表示市场主力资本对标的物的价值判断，
    而最高价和最低价往往反应了市场投机者的情绪，同时合理考虑这样的多方情绪可以更好的衡量市场的动量变化。
    
参考：
    https://zhuanlan.zhihu.com/p/96888358
    https://zhuanlan.zhihu.com/p/379269953
    
说白了：
    就是算当前，和，N天前的，收盘价close的差，也就是收益（**间隔Nt天的收益**）
    但是，收盘价close，再优化一下，用(最高+最低+收盘)/3，
    但是，波动值还是嫌太大，所以要取一下log。
"""

mapping = [
    {'name': 'momentum_10d', 'cname':'10日动量',  'days': 10},
    {'name': 'momentum_1m', 'cname':'1月动量','days': 20},
    {'name': 'momentum_3m', 'cname':'3月动量','days': 60},
    {'name': 'momentum_6m', 'cname':'6月动量','days': 120},
    {'name': 'momentum_12m', 'cname':'12月动量','days': 252},
    {'name': 'momentum_24m', 'cname':'24日动量','days': 480}
]


class MomentumFactor(Factor):
    """
        data['turnover_1m'] = data['turnover_rate'].rolling(window=20, min_periods=1).apply(func=np.nanmean)
        data['turnover_3m'] = data['turnover_rate'].rolling(window=60, min_periods=1).apply(func=np.nanmean)
        data['turnover_6m'] = data['turnover_rate'].rolling(window=120, min_periods=1).apply(func=np.nanmean)
        data['turnover_2y'] = data['turnover_rate'].rolling(window=480, min_periods=1).apply(func=np.nanmean)

    """

    def __init__(self):
        super().__init__()

    def name(self):
        return [m['name'] for m in mapping]

    def cname(self):
        return [m['cname'] for m in mapping]

    def calculate(self, stock_codes, start_date, end_date, df_daily=None):
        """
        计算动量，动量，就是往前回溯period个周期，然后算收益，
        由于股票的价格是一个随经济或标的本身经营情况有变化的变量。那么如果变量有指数增长趋势（exponential growth），
        比如 GDP，股票价格，期货价格，则一般取对数，使得 lnGDP 变为线性增长趋势（linear growth），
        为了防止有的价格高低，所以用log方法，更接近，参考：https://zhuanlan.zhihu.com/p/96888358

        本来的动量用的是减法，这里，换成了除法，就是用来解决文中提到的规模不同的问题
        :param period_window:
        :param df:
        :return:
        """
        results = []
        for m in mapping:
            start_date_2years_ago = utils.last_year(start_date, num=2)
            df_daily = datasource_utils.load_daily_data(self.datasource, stock_codes, start_date_2years_ago, end_date)
            adj_close = (df_daily['close'] + df_daily['high'] + df_daily['low']) / 3
            df_daily[m['name']] = np.log(adj_close / adj_close.shift(m['days']))  # shift(1) 往后移，就变成上个月的了
            df_daily = datasource_utils.reset_index(df_daily)  # 设置日期+code为索引
            df_factor = df_daily[m['name']]
            df_factor.dropna(inplace=True)
            results.append(df_factor)
        return results
