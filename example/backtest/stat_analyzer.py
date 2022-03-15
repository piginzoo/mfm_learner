import logging
from collections import namedtuple

import backtrader as bt
from pandas import DataFrame

from utils import utils

logger = logging.getLogger(__name__)

TradeDetail = namedtuple('Trade', ['date', 'buysell', 'pct_chg'])


"""
我关心下面的指标：
 - 到底交易了几次
 - 每次买入多少只，卖出多少只
 - 每只卖出后的情况，赚还是亏
 - 一共买入过多少只
 - 一共卖出过多少只
 - 卖出的盈亏比例
 - 每天，市值情况是否打败了市场
 - 日胜率
 - 周胜率
 - 月胜率
 - 年胜率
"""

class StatAnalyzer(bt.Analyzer):
    """
    尝试用指标，发现很不好用，指标的那些破函数类，比如PctChange，
    所以放弃了，就直接粗暴得到line=>list=>dataframe，然后用dataframe来算了。
    """

    def create_analysis(self):
        self.rets = bt.AutoOrderedDict()

    def stop(self):
        # Broker()
        # 得到broker观察者的市值
        # values = to_list(self._owner.stats.broker.value)
        # pcts = to_pct(values)

        # 指数数据
        index_close = self.datas[0].close  # 指数的数据
        portfolio_close = self.strategy.stats.broker.lines.value  # 市值
        datetime = self.datas[0].datetime

        datetime = datetime.get(size=datetime.buflen()).tolist()
        datetime = [bt.num2date(d) for d in datetime]  # 从int=>date
        index_close = index_close.get(size=len(datetime)).tolist()
        # 不知为何，portfolio_close.buflen()为448，别人是224，且448前224是有数，后244是nan，
        # 而且，get后为空，很诡异，怀疑是bt的bug，懒得排查了，workaround是直接用datetime来强制对齐
        # portfolio_close = portfolio_close.get(size=portfolio_close.buflen()).tolist()
        portfolio_close = portfolio_close.get(size=len(datetime)).tolist()

        assert len(index_close) == len(portfolio_close) == len(datetime)

        df = DataFrame({'index': index_close,
                        'portfolio': portfolio_close},
                       index=datetime)

        def calculate_win_rate(df, frequency):
            """
            计算胜率，由于1天的无法用resample计算，单拎出来
            df有两列，index的close，和，组合的市值，索引是日期
            结果是，计算整体的胜率，比如：
            frequency = 1D，日胜率
            frequency = 1M，月胜率
            frequency = 1Y，年胜率
            """
            if frequency=="1D" or frequency=='D':
                df_pct_change = df.apply(DataFrame.pct_change)
            else:
                df_pct_change = df.resample(frequency).apply(lambda x: (x[-1] - x[0]) / x[0])

            df_pct_change.dropna(inplace=True)
            df_compare = df_pct_change['portfolio'] > df_pct_change['index']
            wins = df_compare.sum() / len(df_compare)
            return wins

        self.rets.win_rate_day = calculate_win_rate(df, '1D')
        self.rets.win_rate_month = calculate_win_rate(df, 'M')
        self.rets.win_rate_year = calculate_win_rate(df, '1Y')

# class StatObserver(bt.Observer):
#     """
#     我要观察，每一天，在broker的持仓中，组合收益，和，指数收益的对比，算胜率
#     在脑波中，可以通过self.stats，可以访问所有的观察者：
#     https://www.backtrader.com/docu/observers-and-statistics/observers-and-statistics/
#     -----------------------------------------------------------------
#     不用考虑日期的lines:datetime，因为next的时候，lines[0]就加入了一个值，
#     保存一下datetime line吧，奇怪？我看其他的Oberver里都没有
#     """
#     lines = ('datetime', 'value',)
#
#     def next(self):
#         # 记录下当前的市值（现金+持仓市值）
#         self.lines.datetime[0] = self._owner.datetime[0]
#         self.lines.value[0] = self._owner.broker.getvalue()
