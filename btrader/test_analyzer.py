import datetime  #
import os.path  # 路径管理
import sys  # 获取当前运行脚本的路径 (in argv[0])

# 导入backtrader框架
import backtrader as bt
from backtrader import AutoOrderedDict


class StatAnalyzer(bt.Analyzer):
    """
    测试所有的交易细节信息
    """

    def create_analysis(self):
        self.rets = bt.AutoOrderedDict()

    def start(self):
        super().start()
        self.strategy
        self.wins = list()
        self.losses = list()

    def notify_trade(self, trade):
        """
        通过trade可以访问到策略，通过策略访问到每天的情况
        :param trade:
        :return:
        """
        if trade.status == trade.Closed:
            if trade.pnlcomm > 0:
                # 盈利加入盈利列表，利润0算盈利
                self.wins.append(trade)
            else:
                # 亏损加入亏损列表
                self.losses.append(trade)

    def stop(self):
        self.rets.total = len(self.wins) + len(self.losses)
        self.rets.wins = len(self.wins)
        self.rets.losses = len(self.losses)


class TestStrategy(bt.Strategy):
    params = (
        # 均线参数设置15天，15日均线
        ('maperiod', 15),
        ('printlog', False),
    )

    def log(self, txt, doprint=True):
        if doprint: print('%s' % (txt))

    def __init__(self):
        # 保存收盘价的引用
        self.dataclose = self.datas[0].close
        # 跟踪挂单
        self.order = None
        # 买入价格和手续费
        self.buyprice = None
        self.buycomm = None
        self.buy_counter = 0
        self.sell_counter = 0
        self.trade_counter = 0
        # 加入均线指标
        self.sma = bt.indicators.SimpleMovingAverage(self.datas[0], period=self.params.maperiod)

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
        self.trade_counter += 1
        self.log('[%d] 交易利润, 毛利润 %.2f, 净利润 %.2f' %
                 (self.trade_counter, trade.pnl, trade.pnlcomm))

    def next(self):
        # 记录收盘价
        # self.log('Close, %.2f' % self.dataclose[0])

        # 如果有订单正在挂起，不操作
        if self.order:
            return

        # 如果没有持仓则买入
        if not self.position:
            # 今天的收盘价在均线价格之上
            if self.dataclose[0] > self.sma[0]:
                # 买入
                self.buy_counter += 1
                self.log('买入单[%d], %.2f' % (self.buy_counter, self.dataclose[0]))
                # 跟踪订单避免重复
                self.order = self.buy()
        else:
            # 如果已经持仓，收盘价在均线价格之下
            if self.dataclose[0] < self.sma[0]:
                # 全部卖出
                self.sell_counter += 1
                self.log('卖出单[%d], %.2f' % (self.sell_counter, self.dataclose[0]))
                # 跟踪订单避免重复
                self.order = self.sell()

    # 测略结束时，多用于参数调优
    def stop(self):
        self.log('(均线周期 %2d)期末资金 %.2f' % (self.params.maperiod, self.broker.getvalue()), doprint=True)


# python -m btrader.test_analyzer
if __name__ == '__main__':
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    # Cerebro引擎在后台创建broker(经纪人)，系统默认资金量为10000

    # 为Cerebro引擎添加策略
    # cerebro.addstrategy(TestStrategy)

    # 为Cerebro引擎添加策略, 优化策略
    # 使用参数来设定10到31天的均线,看看均线参数下那个收益最好
    strats = cerebro.optstrategy(
        TestStrategy,
        maperiod=range(10, 31))

    # 获取当前运行脚本所在目录
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    # 拼接加载路径
    # https://raw.githubusercontent.com/jackvip/backtrader/master/orcl-1995-2014.txt
    datapath = os.path.join(modpath, '../data/orcl-1995-2014.txt')

    # 创建交易数据集
    data = bt.feeds.YahooFinanceCSVData(
        dataname=datapath,
        # 数据必须大于fromdate
        fromdate=datetime.datetime(2000, 1, 1),
        # 数据必须小于todate
        todate=datetime.datetime(2000, 12, 31),
        reverse=False)

    # 加载交易数据
    cerebro.adddata(data)

    # 设置投资金额1000.0
    cerebro.broker.setcash(10000.0)

    # 每笔交易使用固定交易量
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    # 设置佣金为0.0
    cerebro.broker.setcommission(commission=0.0)

    # 添加分析对象
    import backtrader.analyzers as btay  # 添加分析函数

    cerebro.addanalyzer(btay.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days)  # 夏普指数
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')  # 回撤分析
    cerebro.addanalyzer(StatAnalyzer, _name="SA")
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='TA')

    results = cerebro.run()
    print("2000-1-1~2000-12-31,252个交易日，28次交易，8次赢，19次输，1次未结束")
    print("夏普比:", results[0][0].analyzers.sharpe.get_analysis())
    print("统计1:", results[0][0].analyzers.SA.get_analysis())


    # print("统计2:", results[0][0].analyzers.TA.get_analysis())

    def printkv(k, v, tabs):
        if type(v) == AutoOrderedDict:
            print("\t"*tabs,k)
            for kk, vv in v.items():
                printkv(kk, vv, tabs + 1)
        else:
            print("\t" * tabs, k, v)


    print("统计2：")
    for k, v in results[0][0].analyzers.TA.get_analysis().items():
        printkv(k, v, 0)
        print("-"*80)


    """
    total_total	10              总共几次交易
    total_open	1               未完成的交易
    total_closed	9           完成的交易
    streak_won_current	0       
    streak_won_longest	3       最长连续赢：3次           
    streak_lost_current	1       
    streak_lost_longest	1       最长连续输：1次
    pnl_gross_total	23.16975        收益率 23%
    pnl_gross_average	2.574416667 平均收益率：2.6%
    pnl_net_total	23.16975        净收益率：    
    pnl_net_average	2.574416667     平均净收益率：
    won_total	6                   赢次数
    won_pnl_total	42.99625        赢的合计收益
    won_pnl_average	7.166041667     赢的平均收益
    won_pnl_max	16.645              赢的最大单次收益
    lost_total	3                   输次数
    lost_pnl_total	-19.8265        输总负收益    
    lost_pnl_average	-6.608833333输平均负收益
    """
