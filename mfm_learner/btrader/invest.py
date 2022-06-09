import quantstats as qs

from utils import utils

utils.init_logger()

# 导入backtrader框架
import backtrader as bt

# 创建策略继承bt.Strategy
from backtrader.feeds import PandasData

from datasource import datasource_factory
from mfm_learner.example.factor_backtester import comply_backtrader_data_format

import matplotlib.pyplot as plt
from backtrader.plot import Plot_OldSync


class MyPlot(Plot_OldSync):
    def show(self):
        plt.savefig("debug/backtrader回测.jpg")


class MyStrategy(bt.Strategy):
    """
    定投，月投，
    - 如果市场在下跌，就补仓100%
    - 如果市场在上涨，就补仓50%
    """

    params = (
        ('period', (10, 22, 30, 60)),  # 补仓周期
        ('invest', 0),
        ('trade_days', 0)
    )

    def log(self, txt, doprint=None):
        # 记录策略的执行日志
        # dt = dt or self.datas[0].datetime.date(0)
        if doprint: print('%s' % (txt))

    def __init__(self):
        # 保存收盘价的引用
        self.last_price = 0
        self.current_day = 0
        self.time = 0
        duration = int(self.params.trade_days / self.params.period)
        self.amount_per_period = int(self.params.invest / duration)
        print("总投资：%d，投资期数：%d, 每期投资: %d" % (self.params.invest, duration, self.amount_per_period))

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

        self.time += 1
        self.current_day = 0
        current_price = self.data0.close[0]

        if self.last_price and current_price < self.last_price:
            amount = self.amount_per_period  # * 0.5
            # print("[%r] 下跌趋势，上次价格%.2f, 现价%.2f，买入一半 %.2f" % (current_date, self.last_price, current_price, amount))
        else:
            amount = self.amount_per_period
            if self.last_price is None: self.last_price = 0
            # print("[%r] 上涨趋势，上次价格%.2f, 现价%.2f，买入 %.2f" % (current_date, self.last_price, current_price, amount))

        self.last_price = current_price

        # # 如果有订单正在挂起，不操作
        # if self.order:
        #     return

        self.buy(size=int(amount / current_price))
        # print("购入 %d 股, %d 次" % (int(amount / current_price),self.time))

    # 测略结束时，多用于参数调优
    def stop(self):
        self.log('调仓期[%r]的期末资金 ： %.2f' % (self.params.period, self.broker.getvalue()), doprint=True)


def main():
    trade_days = datasource_factory.create('tushare').trade_cal(start_date, end_date)
    trade_days = len(trade_days)

    # 创建Cerebro引擎
    cerebro = bt.Cerebro()

    # # 使用参数来设定10到31天的均线,看看均线参数下那个收益最好
    strats = cerebro.optstrategy(
        MyStrategy,
        trade_days=trade_days,
        period=period,
        invest=total_invest
    )

    # df_index = datasource_factory.get().index_daily(code, start_date, end_date)
    df_index = datasource_factory.create('akshare').fund_daily(code, start_date, end_date)

    df_index = comply_backtrader_data_format(df_index)

    data = PandasData(dataname=df_index, fromdate=df_index.index[0], todate=df_index.index[-1], plot=True, close=0)
    cerebro.adddata(data, name=code)
    print("初始化 [%s] 数据到脑波：%d 条" % (code, len(df_index)))

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
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')  # 加入PyFolio分析者,这个是为了做quantstats分析用

    results = cerebro.run(maxcpus=1)

    def format_print(title, results):
        print(title, ":")
        # print(results[0].analyzers)
        for year, value in results.items():
            print("\t %s : %r" % (year, value))

    for i, result in enumerate(results):
        print("-" * 80)
        print("%d天调仓期结果：" % period[i])
        format_print("回撤:", result[0].analyzers.DW.get_analysis())
        format_print("收益:", result[0].analyzers.returns.get_analysis())
        format_print("期间:", result[0].analyzers.period_stats.get_analysis())
        format_print("年化:", result[0].analyzers.annual.get_analysis())
        quant_statistics(result[0], period[i], code, name)

    # cerebro.plot(plotter=MyPlot(), style="candlestick", iplot=False)


def quant_statistics(strat, period, code, name):
    portfolio_stats = strat.analyzers.getbyname('PyFolio')  # 得到PyFolio分析者实例
    # 以下returns为以日期为索引的资产日收益率系列
    returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()

    returns.index = returns.index.tz_convert(None)  # 索引的时区要设置一下，否则出错

    # 输出html策略报告,rf为无风险利率
    qs.reports.html(returns,
                    output='debug/stats_{}{}_{}.html'.format(code, name, period),
                    title='{}日调仓的定投[{}{}]基金的绩效报告'.format(period, code, name), rf=0.0)

    print(qs.reports.metrics(returns=returns, mode='full'))
    df = qs.reports.metrics(returns=returns, mode='full', display=False)
    print("返回的QuantStats报表：\n%r", df)
    qs.reports.basic(returns)


# python -m btrader.invest
if __name__ == '__main__':
    code = "000905.SH"  # 中证500
    code = "000300.SH"  # 沪深300
    code = '003327'  # 基金代码 001938：时代先锋 002943 ： 广发多因子
    name = '万家鑫景A'
    start_date = "20150101"
    end_date = "20210115"
    period = (10, 22, 30, 60)
    total_invest = 100000

    main()
