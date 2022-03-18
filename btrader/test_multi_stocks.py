from datasource import datasource_factory
from example.backtest import data_loader
from example.backtest.winrate_analyzer import WinRateAnalyzer, StatObserver
from utils import utils

"""
测试多只股票的，每天的胜率，
实现方法是使用StatAnalyser，
做法是，用broker中的value，和，传入的基准做比较pct_change，看胜率
"""

datasource = datasource_factory.get()
import backtrader as bt


# 创建策略
class SmaCross(bt.Strategy):
    # 可配置策略参数
    params = dict(
        pfast=2,  # 短期均线周期
        pslow=5,  # 长期均线周期
        pstake=1000  # 单笔交易股票数目
    )

    def __init__(self):
        sma1 = [bt.ind.SMA(d, period=self.p.pfast) for d in self.datas]
        sma2 = [bt.ind.SMA(d, period=self.p.pslow) for d in self.datas]
        self.crossover = {
            d: bt.ind.CrossOver(s1, s2)
            for d, s1, s2 in zip(self.datas, sma1, sma2)
        }

    def next(self):
        for d in self.datas:
            if not self.getposition(d).size:
                if self.crossover[d] > 0:
                    self.buy(data=d, size=self.p.pstake)  # 买买买
            elif self.crossover[d] < 0:
                self.close(data=d)  # 卖卖卖


# python -m btrader.test_multi_stocks
if __name__ == '__main__':
    ##########################
    # 主程序开始
    #########################

    utils.init_logger()

    start_date = '20190101'
    end_date = '20200101'
    index_code = '000905.SH'

    cerebro = bt.Cerebro()

    stock_codes = datasource.index_weight(index_code, start_date, end_date)
    stock_codes = stock_codes[:10]

    # 加载指数数据到脑波
    df_benchmark_index = data_loader.load_index_data(cerebro, index_code, start_date, end_date)

    # 加载股票数据到脑波
    data_loader.load_stock_data(cerebro, start_date, end_date, stock_codes, atr_period=10)

    cerebro.broker.setcash(1000000)
    cerebro.broker.setcommission(commission=0.002)
    cerebro.addanalyzer(WinRateAnalyzer)
    cerebro.addobserver(StatObserver)
    cerebro.addstrategy(SmaCross)  # 注入策略
    cerebro.run()

    # 最终收益或亏损
    pnl = cerebro.broker.get_value() - 1000000
    print('Profit ... or Loss: {:.2f}'.format(pnl))