# 导入backtrader框架
import datetime
import os
import sys

import backtrader as bt

"""
参考：http://backtrader.com.cn/docu/#301
"""

# 创建策略继承bt.Strategy
class TestStrategy(bt.Strategy):
    """
    Strategy类有一个变量position保存当前持有的资产数量（可以理解为金融术语中的头寸）
    """

    def log(self, txt, dt=None):
        # 记录策略的执行日志
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # 保存收盘价的引用
        self.dataclose = self.datas[0].close
        # 跟踪挂单
        self.order = None

    def next(self):
        # 记录收盘价
        self.log('Close, %.2f' % self.dataclose[0])

        # 如果有订单正在挂起，不操作
        if self.order:
            return

        # 如果没有持仓则买入
        if not self.position:
            # 今天的收盘价 < 昨天收盘价
            if self.dataclose[0] < self.dataclose[-1]:
                # 昨天收盘价 < 前天的收盘价
                if self.dataclose[-1] < self.dataclose[-2]:
                    # 买入
                    self.log('买入, %.2f' % self.dataclose[0])
                    # 订单被以”市价”成交了。 Broker（经纪人，之前提到过）使用了下一个交易日的开盘价，因为是broker在当前的交日易收盘后天提交的订单，下一个交易日开盘价是他接触到的第一个价格
                    self.buy()
        else:
            # 如果已经持仓，且当前交易数据量在买入后5个单位后
            # 没有将柱的下标传给next()方法，怎么知道已经经过了5个柱了呢？ 这里用了Python的len()方法获取它Line数据的长度。
            # 交易发生时记下它的长度，后边比较大小，看是否经过了5个柱。
            if len(self) >= (self.bar_executed + 5):
                # 全部卖出
                self.log('卖出, %.2f' % self.dataclose[0])
                # 跟踪订单避免重复
                self.order = self.sell()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # broker 提交/接受了，买/卖订单则什么都不做
            return

        # 检查一个订单是否完成
        # 注意: 当资金不足时，broker会拒绝订单
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('已买入, %.2f' % order.executed.price)
            elif order.issell():
                self.log('已卖出, %.2f' % order.executed.price)

            # 记录当前交易数量
            # 没有将柱的下标传给next()方法，怎么知道已经经过了5个柱了呢？ 这里用了Python的len()方法获取它Line数据的长度。
            # 交易发生时记下它的长度，后边比较大小，看是否经过了5个柱。
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

if __name__ == '__main__':
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    # Cerebro引擎在后台创建broker(经纪人)，系统默认资金量为10000

    # 获取当前运行脚本所在目录
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    # 拼接加载路径
    datapath = os.path.join(modpath, '../data/orcl-1995-2014.txt')

    # 创建交易数据集
    data = bt.feeds.YahooFinanceCSVData(
        dataname=datapath,
        # 数据必须大于fromdate
        fromdate=datetime.datetime(2000, 1, 1),
        # 数据必须小于todate
        todate=datetime.datetime(2000, 12, 31),
        reverse=False)

    # 为Cerebro引擎添加策略
    cerebro.addstrategy(TestStrategy)

    # 这比费用叫做佣金。让我们设定一个常见的费率0.1 %，买卖都要收（经纪人就是这么贪）
    # 设置佣金为0.001,除以100去掉%号
    cerebro.broker.setcommission(commission=0.001)  # 0.001即是0.1%

    # 加载交易数据
    cerebro.adddata(data)

    # 设置投资金额100000.0
    cerebro.broker.setcash(100000.0)
    # 引擎运行前打印期出资金
    print('组合期初资金: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    # 引擎运行后打期末资金
    print('组合期末资金: %.2f' % cerebro.broker.getvalue())
