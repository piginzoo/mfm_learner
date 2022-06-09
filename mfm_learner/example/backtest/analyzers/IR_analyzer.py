import logging
from collections import namedtuple

import backtrader as bt
from pandas import DataFrame

logger = logging.getLogger(__name__)

TradeDetail = namedtuple('Trade', ['date', 'buysell', 'pct_chg'])


class WinRateAnalyzer(bt.Analyzer):
    """
    尝试用backtrader的指标（Indicator），发现很不好用，指标的那些破函数类，比如PctChange，
    所以放弃了，就直接粗暴得到line=>list=>dataframe，然后用dataframe来算了。

    2022.4.8，增加了正负胜率，比之前和指数比，这里再增加一个正负的，虽然，TradeAnalyzer里有胜负数据，
    但是，我对那数据心存怀疑，还是自己实现一个靠谱，
    """

    def create_analysis(self):
        self.rets = bt.AutoOrderedDict()

    def calculate_win_rate_with_baseline(self, df, frequency):
        """
        计算，和基准（往往是指数的收益率），来比较胜率
        计算胜率，由于1天的无法用resample计算，单拎出来
        df有两列，index的close，和，组合的市值，索引是日期
        结果是，计算整体的胜率，比如：
        frequency = 1D，日胜率
        frequency = 1M，月胜率
        frequency = 1Y，年胜率
        """
        return self.__calculate_win_rate(df, frequency, True)

    def calculate_win_rate_positive_negtive(self, df, frequency):
        """
        计算每天对应的正收益对比，即所有的pct_change
        计算胜率，由于1天的无法用resample计算，单拎出来
        df有两列，index的close，和，组合的市值，索引是日期
        结果是，计算整体的胜率，比如：
        frequency = 1D，日胜率
        frequency = 1M，月胜率
        frequency = 1Y，年胜率
        """
        return self.__calculate_win_rate(df, frequency, False)

    def __calculate_win_rate(self, df, frequency, is_use_index_as_baseline):
        """
        计算每天对应的正收益对比，即所有的pct_change
        计算胜率，由于1天的无法用resample计算，单拎出来
        df有两列，index的close，和，组合的市值，索引是日期
        结果是，计算整体的胜率，比如：
        frequency = 1D，日胜率
        frequency = 1M，月胜率
        frequency = 1Y，年胜率

        is_use_index: 是否使用指数收益做基准
        """
        if frequency == "1D" or frequency == 'D':
            df_pct_change = df.apply(DataFrame.pct_change)
        else:
            df_pct_change = df.resample(frequency).apply(lambda x: (x[-1] - x[0]) / x[0])

        df_pct_change.dropna(inplace=True)
        if is_use_index_as_baseline:
            df_compare = df_pct_change['portfolio'] >= df_pct_change['index']
        else:
            df_compare = df_pct_change['portfolio'] >= 0

        wins = df_compare.sum()
        fails = len(df_compare) - wins
        win_rate = wins / (wins + fails)
        return wins, fails, win_rate

    def stop(self):
        # Broker()
        # 得到broker观察者的市值
        # values = to_list(self._owner.stats.broker.value)
        # pcts = to_pct(values)

        # 指数数据
        index_close = self.datas[0].close  # 指数的数据
        portfolio_close = self.strategy.stats.broker.lines.value  # 通过broker获得组合的市值
        datetime = self.datas[0].datetime

        datetime = datetime.get(size=datetime.buflen()).tolist() # 得到所有的日期
        datetime = [bt.num2date(d) for d in datetime]  # 从int=>date # 转成date
        index_close = index_close.get(size=len(datetime)).tolist() # 按照交易日，得到所有的对应的指数收盘价
        # 不知为何，portfolio_close.buflen()为448，别人是224，且448前224是有数，后244是nan，
        # 而且，get后为空，很诡异，怀疑是bt的bug，懒得排查了，workaround是直接用datetime来强制对齐
        # portfolio_close = portfolio_close.get(size=portfolio_close.buflen()).tolist()
        portfolio_close = portfolio_close.get(size=len(datetime)).tolist()

        assert len(index_close) == len(portfolio_close) == len(datetime)

        df = DataFrame({'index': index_close,
                        'portfolio': portfolio_close},
                       index=datetime)

        self.rets.win_day, self.rets.fail_day, self.rets.win_rate_day = self.calculate_win_rate_with_baseline(df, '1D')
        self.rets.win_month, self.rets.fail_month, self.rets.win_rate_month = self.calculate_win_rate_with_baseline(df,
                                                                                                                    'M')
        self.rets.win_year, self.rets.fail_year, self.rets.win_rate_year = self.calculate_win_rate_with_baseline(df,
                                                                                                                 '1Y')
        self.rets.positive_day, self.rets.negative_day, self.rets.pnl_rate_day = self.calculate_win_rate_positive_negtive(
            df, '1D')
        self.rets.positive_month, self.rets.negative_month, self.rets.pnl_rate_month = self.calculate_win_rate_positive_negtive(
            df, 'M')
        self.rets.positive_year, self.rets.negative_year, self.rets.pnl_rate_year = self.calculate_win_rate_positive_negtive(
            df, '1Y')

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
