import logging

import numpy as np

from datasource import datasource_utils
from example.factors.factor import Factor

logger = logging.getLogger(__name__)
period_window = 10

"""

动量因子，动量因子是指与股票的价格和交易量变化相关的因子，常见的动量因子：一个月动量、3个月动量等。
计算个股（或其他投资标的）过去N个时间窗口的收益回报：
    adj_close = (high + low + close)/3
    adj_return = adj_close_t - adj_close_{t-n}
来计算受盘中最高价和最低价的调整的调整收盘价动量，逻辑是，在日线的层面上收盘价表示市场主力资本对标的物的价值判断，
而最高价和最低价往往反应了市场投机者的情绪，同时合理考虑这样的多方情绪可以更好的衡量市场的动量变化。
    
参考：
    https://zhuanlan.zhihu.com/p/96888358
"""


class MomentumFactor(Factor):

    def __init__(self):
        super().__init__()

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
        if df_daily is None:
            df_daily = datasource_utils.load_daily_data(self.datasource, stock_codes, start_date, end_date)

        adj_close = (df_daily['close'] + df_daily['high'] + df_daily['low']) / 3
        df_daily['momentum'] = np.log(adj_close / adj_close.shift(period_window))  # shift(1) 往后移，就变成上个月的了
        df_daily = datasource_utils.reset_index(df_daily)
        return df_daily['momentum']
