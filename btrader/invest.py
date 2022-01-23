from utils import utils
from utils.utils import MyPlot

utils.init_logger()

# 导入backtrader框架
import backtrader as bt

# 创建策略继承bt.Strategy
from backtrader.feeds import PandasData

from datasource import datasource_factory
from example.factor_backtester import comply_backtrader_data_format


class MyStrategy(bt.Strategy):
    """
    定投，月投，
    - 如果市场在下跌，就补仓100%
    - 如果市场在上涨，就补仓50%
    """

    params = (
        # 补仓周期
        ('period', 22),
        ('invest', 0),
    )

    def log(self, txt, doprint=None):
        # 记录策略的执行日志
        # dt = dt or self.datas[0].datetime.date(0)
        if doprint: print('%s' % (txt))

    def __init__(self):
        # 保存收盘价的引用
        self.last_price = 0
        self.current_day = 0
        self.time=0

    # 订单状态通知，买入卖出都是下单
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # broker 提交/接受了，买/卖订单则什么都不做
            return

        # 检查一个订单是否完成
        # 注意: 当资金不足时，broker会拒绝订单
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    '已买入, 价格: %.2f, 费用: %.2f, 佣金 %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            elif order.issell():
                self.log('已卖出, 价格: %.2f, 费用: %.2f, 佣金 %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))
            # 记录当前交易数量
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('订单取消/保证金不足/拒绝')

        # 其他状态记录为：无挂起订单
        self.order = None

    # 交易状态通知，一买一卖算交易
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('交易利润, 毛利润 %.2f, 净利润 %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):

        current_date = utils.date2str(self.data0.datetime.datetime(0))

        if self.current_day < self.params.period:
            self.current_day += 1
            # print("当前 %d, 调仓期 %d" % (self.current_day , self.params.period))
            return

        self.time+=1
        self.current_day = 0
        current_price = self.data0.close[0]

        if self.last_price and current_price < self.last_price:
            amount = self.params.invest * 0.5
            print("[%r] 下跌趋势，上次价格%.2f, 现价%.2f，买入一半 %.2f" % (current_date, self.last_price, current_price, amount))
        else:
            amount = self.params.invest
            if self.last_price is None: self.last_price = 0
            print("[%r] 上涨趋势，上次价格%.2f, 现价%.2f，买入 %.2f" % (current_date, self.last_price, current_price, amount))

        self.last_price = current_price

        # # 如果有订单正在挂起，不操作
        # if self.order:
        #     return

        self.buy(size=int(amount / current_price))
        print("购入 %d 股, %d 次" % (int(amount / current_price),self.time))

    # 测略结束时，多用于参数调优
    def stop(self):
        self.log('(均线周期 %2d)期末资金 %.2f' % (self.params.period, self.broker.getvalue()), doprint=True)


# python -m btrader.invest
if __name__ == '__main__':
    index_code = "000905.SH"  # 中证500
    index_code = "000300.SH"  # 沪深300
    start_date = "20170101"
    end_date = "20211201"
    period = 18
    total_invest = 1000000
    duration = int(abs((utils.str2date(start_date) - utils.str2date(end_date)).days / 30))
    amount_per_period = int(total_invest / duration)
    print("总投资：%d，投资期数：%d, 每期投资: %d" % (total_invest, duration, amount_per_period))

    # 创建Cerebro引擎
    cerebro = bt.Cerebro()

    # # 使用参数来设定10到31天的均线,看看均线参数下那个收益最好
    strats = cerebro.addstrategy(
        MyStrategy,
        period=period,
        invest=amount_per_period
    )

    d_start_date = utils.str2date(start_date)  # 开始日期
    d_end_date = utils.str2date(end_date)  # 结束日期
    # import pdb; pdb.set_trace()
    df_index = datasource_factory.get().index_daily(index_code, start_date, end_date)
    df_index = comply_backtrader_data_format(df_index)

    data = PandasData(dataname=df_index, fromdate=df_index.index[0], todate=df_index.index[-1], plot=True)
    cerebro.adddata(data, name=index_code)
    print("初始化 [%s] 数据到脑波：%d 条" % (index_code, len(df_index)))

    # 设置投资金额1000.0
    cerebro.broker.setcash(total_invest)


    # # 每笔交易使用固定交易量
    # cerebro.addsizer(bt.sizers.FixedSize, stake=10)

    # 设置佣金为0.0
    cerebro.broker.setcommission(commission=0.00035)

    # 添加分析对象
    import backtrader.analyzers as btay  # 添加分析函数

    cerebro.addanalyzer(btay.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days)  # 夏普指数
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')  # 回撤分析
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.PeriodStats, _name='period_stats')
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='annual')
    results = cerebro.run()


    def format_print(title, results, key):
        print(title, ":")
        print(results[0].analyzers)
        for year, value in results[0].analyzers.get(key).get_analysis().items():
            print("\t %s : %r" % (year, value))

    print("总资金:", cerebro.broker.getvalue())
    print("现金：",cerebro.broker.getcash())
    print("收益率: %.2f%%" % ((cerebro.broker.getvalue() - total_invest) * 100 / total_invest))
    print("回撤:", results[0].analyzers.DW.get_analysis())
    print("收益:", results[0].analyzers.returns.get_analysis())
    # format_print("收益", results, "returns")
    print("期间:", results[0].analyzers.period_stats.get_analysis())
    print("年化:")
    for year, value in results[0].analyzers.annual.get_analysis().items():
        print("\t %s : %.0f%%" % (year, value * 100))

    cerebro.plot(plotter=MyPlot(), style="candlestick", iplot=False)
