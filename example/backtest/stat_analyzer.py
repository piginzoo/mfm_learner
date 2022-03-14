import backtrader as bt
from backtrader.analyzers import TradeAnalyzer
from backtrader.observers import BuySell, Trade


class StatAnalyzer(bt.Analyzer):
    lines = ('buy', 'sell',)

    def create_analysis(self):
        self.rets = bt.AutoOrderedDict()
        BuySell()
        TradeAnalyzer()
        Trade()

    def start(self):
        super().start()
        self.pnlWins = list()
        self.pnlLosses = list()

    def notify_trade(self, trade):
        """
        通过trade可以访问到策略，通过策略访问到每天的情况
        :param trade:
        :return:
        """

        if trade.status == trade.Closed:
            if trade.pnlcomm > 0:
                # 盈利加入盈利列表，利润0算盈利
                self.pnlWins.append(trade.pnlcomm)
            else:
                # 亏损加入亏损列表
                self.pnlLosses.append(trade.pnlcomm)

    def stop(self):
        self.rets.total = len(self.pnlWins) + len(self.pnlLosses)
        self.rets.wins = len(self.pnlWins)
        self.rets.losses = len(self.pnlLosses)


class StatObserver(bt.Analyzer):
    """
    我要观察，每一个调仓日，都是哪些股票，被买入，哪些被卖出，收益多少
    date
        List<code    sell/buy    profit>
    """
    pass