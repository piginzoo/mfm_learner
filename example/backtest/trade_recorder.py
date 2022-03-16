from backtrader import Trade

from example.backtest.trade_listener import TradeListener


class TradeRecorder(TradeListener):

    def __init__(self):
        self.current_stocks = []

    def on_trade(self, trade):
        stock_code = trade.data._name

        # import pdb;pdb.set_trace()
        # 新创建交易，那么就是认为是买入
        if trade.status == Trade.Open:
            self.current_stocks.append(stock_code)

        # 关闭交易，相当于卖出
        if trade.status == Trade.Closed:
            self.current_stocks.remove(stock_code)

    def get_stocks(self):
        return self.current_stocks
