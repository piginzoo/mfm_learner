from example.backtest.buysell_listener import BuySellListener


class BuySellRecorder(BuySellListener):

    def __init__(self):
        self.current_stocks = []
        self.postion_change_rate = []

    def on_buy(self,stock_code,price):
        self.current_stocks.append(stock_code)

    def on_sell(self,stock_code):
        self.current_stocks.remove(stock_code)

    def get(self):
        return self.current_stocks