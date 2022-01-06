import logging
import math
import time

import pandas as pd
from backtrader.feeds import PandasData

from example import factor_combiner
from utils import utils, tushare_dbutils

utils.init_logger()
import backtrader as bt  # 引入backtrader框架
import backtrader.analyzers as btay  # 添加分析函数
import numpy as np

"""
用factor_tester.py中合成的多因子，做选择股票的策略 ，去选择中证500的股票，跑收益率回测。使用backtrader来做回测框架。
参考：
- https://zhuanlan.zhihu.com/p/351751730
"""

logger = logging.getLogger(__name__)


class Percent(bt.Sizer):
    """
    定义每次卖出的股票的百分比
    """
    params = (
        ('percents', 10),
        ('retint', False),  # 返回整数
    )

    def __init__(self):
        pass

    def _getsizing(self, comminfo, cash, data, isbuy):
        position = self.broker.getposition(data)
        if not position:
            size = cash / data.close[0] * (self.params.percents / 100)
        else:
            size = position.size

        if self.p.retint:
            size = int(size)

        return size


class CombineFactorStrategy(bt.Strategy):
    """
    我自己的多因子策略，即，用我的多因子来进行选股，股票池是中证500，每一个选股周期，我都要根据当期数据，去计算因子暴露（因子值），
    然后根据因子值，对当前期中证500股票池中的股票进行排序，（这个期间中证500可能备选股票可能会变化）
    然后，选择前100名进行投资，对比新旧100名中，卖出未在list中，买入list中的，如此进行3年、5年投资，看回测收益率。
    -------------
    实现细节：
    - 回测期间使用2014.1.1~2018.12.31，跨度5年，主要是想看看，2015年股灾、2016年之后风格轮动后的效果对比，
    - 后续的期间，是可以使用之前的数据了，比如回测的是2017.3的，就可以使用2017.2月的数据了
    - 每次选股前，都要重新计算因子暴露（敞口、因子值），所以计算量还不小呢，即每次都要用一个滑动窗口（12个月）算
    - 单因子有负向的，比如市值因子，那么在因子合成的时候，应该怎么处理负向的呢？

    (2020.3.2)--------10个交易日----------(2020.3.15)
    |
    当前，通过因子，去预测3.15的收益率，
    是通过看3.2号的因子情况。
    """

    # 可配置策略参数
    params = dict(
        period=30,  # 均线周期
        stake=100,  # 单笔交易股票数目
    )

    def __init__(self, stock_index, period, total, factors):
        self.stock_index = stock_index  # 使用的股票池
        self.current_stocks = []  # 当前持仓
        self.period = period  # 调仓周期
        self.current_day = 0  # 当前周期内的天数
        self.count = 0
        self.total = total
        self.factors = factors
        logger.debug("调仓期:%d，股票池：%s, 交易日： %d 天", period, stock_index, total)

    def __print_broker(self):
        # logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~")
        # logger.debug('|  当前总资产:%.2f', self.broker.getvalue())
        # logger.debug('|  当前总头寸:%.2f', self.broker.getcash())
        # logger.debug('|  当前持仓量:%.2f', self.broker.getposition(self.data).size)
        # logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~")
        pass

    def next(self):
        """
        每天都会回调，我们的逻辑是：
        - 是否到达调仓周期，如果未到忽略
        - 找出此期间中证500只包含的股票池中的股票
        - 根据每支股票的当日的数据，计算每一个单因子值
        - 讲多个因子值，合称为一个因子
        - 根据每只股票的合成因子值排序，找出最好的100只备选股
        - 卖出持仓的，却不在备选股中的那些股票
        - 买入那些在备选股中，却不在持仓中的股票
        - TODO：目前不考虑已购入股票的仓位调整，未来考虑
        异常处理：
        - 如果股票停盘，会顺序买入下一位排名的股票
        - 每次都是满仓，即用卖出的股票头寸，全部购入新的股票，头寸仅在新购入股票中平均分配
        - 如果没有头寸，则不再购买（这种情况应该不会出现）
        """
        logger.debug("已经处理了%d个数据, 总共有%d个数据", len(self), self.data.buflen())

        # logger.debug("--------------------------------------------------")
        # logger.debug('当前可用资金:%r', self.broker.getcash())
        # logger.debug('当前总资产:%r', self.broker.getvalue())
        # logger.debug('当前持仓量:%r', self.broker.getposition(self.data).size)
        # logger.debug('当前持仓成本:%r', self.broker.getposition(self.data).price)
        # logger.debug('当前持仓量:%r', self.getposition(self.data).size)
        # logger.debug('当前持仓成本:%r', self.getposition(self.data).price)
        # logger.debug("--------------------------------------------------")

        # 回测最后一天不进行买卖,datas[0]就是当天, bugfix:  之前self.datas[0].date(0)不行，因为df.index是datetime类型的
        current_date = self.datas[0].datetime.datetime(0)

        self.current_day += 1
        self.count += 1

        if self.current_day < self.period: return

        logger.debug("-" * 50)

        self.current_day = 0

        factor = self.factors.loc[current_date]
        logger.debug("交易日：%r , %d/%d", utils.date2str(current_date), self.count, self.total)
        if np.isnan(factor).all():
            logger.debug("%r 日的因子全部为NAN，忽略当日", utils.date2str(current_date))
            return

        factor = factor.dropna()
        # logger.debug("当天的因子为：%r", factor)
        # 选择因子值前20%
        select_stocks = factor.index[:math.ceil(0.2 * len(factor))]
        logger.debug("此次选中的股票为：%r", ",".join(select_stocks.tolist()))

        # 以往买入的标的，本次不在标的中，则先平仓
        # "常规下单函数主要有 3 个：买入 buy() 、卖出 sell()、平仓 close() "
        to_sell_stocks = set(self.current_stocks) - set(select_stocks)

        logger.debug("卖出股票：%r", to_sell_stocks)
        for sell_stock in to_sell_stocks:
            # 根据名字获得对应那只股票的数据
            stock_data = self.getdatabyname(sell_stock)

            # size = self.getsizing(stock_data,isbuy=False)
            # self.sell(data=stock_data,exectype=bt.Order.Limit,size=size)
            size = self.getposition(stock_data, self.broker).size
            self.close(data=stock_data, exectype=bt.Order.Limit)
            self.current_stocks.remove(sell_stock)
            logger.debug('平仓股票 %s : 卖出%r股', stock_data._name, size)

        logger.debug("卖出%d只股票，剩余%d只持仓", len(to_sell_stocks), len(self.current_stocks))

        self.__print_broker()

        # 每只股票买入资金百分比，预留2%的资金以应付佣金和计算误差
        buy_percentage = (1 - 0.02) / len(select_stocks)

        # 得到可以用来购买的金额,控制仓位在0.6
        buy_amount = buy_percentage * self.broker.getcash()

        for buy_stock in select_stocks:

            # 防止股票不在数据集中
            if buy_stock not in self.getdatanames():
                continue

            # 如果选中的股票在当前的持仓中，就忽略
            if buy_stock in self.current_stocks:
                logger.debug("%s 在持仓中，不动", buy_stock)
                continue

            # 根据名字获得对应那只股票的数据
            stock_data = self.getdatabyname(buy_stock)
            open_price = stock_data.open[0]

            # 按次日开盘价计算下单量，下单量是100（手）的整数倍
            size = math.ceil(buy_amount / open_price)
            logger.debug("购入股票[%s 股价%.2f] %d股，金额:%.2f", buy_stock, open_price, size, buy_amount)
            self.buy(data=stock_data, size=size, price=open_price, exectype=bt.Order.Limit)
            self.current_stocks.append(buy_stock)

        self.__print_broker()

    # 记录交易执行情况（可省略，默认不输出结果）
    def notify_order(self, order):
        # logger.debug('订单状态：%r', order.Status[order.status])
        # print(order)

        # 如果order为submitted/accepted,返回空
        if order.status in [order.Submitted, order.Accepted]:
            # logger.debug('订单状态：%r', order.Status[order.status])
            return

        # 如果order为buy/sell executed,报告价格结果
        if order.status in [order.Completed]:
            if order.isbuy():
                logger.debug('成功买入: 股票[%s],价格[%.2f],成本[%.2f],手续费[%.2f]',
                             order.data._name,
                             order.executed.price,
                             order.executed.value,
                             order.executed.comm)

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                bt.OrderData
                logger.debug('成功卖出: 股票[%s],价格[%.2f],成本[%.2f],手续费[%.2f]',
                             order.data._name,
                             order.executed.price,
                             order.executed.value,
                             order.executed.comm)

            self.bar_executed = len(self)

        # 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            """
            Order.Created：订单已被创建；
            Order.Submitted：订单已被传递给经纪商 Broker；
            Order.Accepted：订单已被经纪商接收；
            Order.Partial：订单已被部分成交；
            Order.Complete：订单已成交；
            Order.Rejected：订单已被经纪商拒绝；
            Order.Margin：执行该订单需要追加保证金，并且先前接受的订单已从系统中删除；
            Order.Cancelled (or Order.Canceled)：确认订单已经被撤销；
            Order.Expired：订单已到期，其已经从系统中删除 。
            """
            logger.debug('交易失败，股票[%s]订单状态：%r', order.data._name, order.Status[order.status])

        self.order = None

    # 记录交易收益情况（可省略，默认不输出结果）
    def notify_trade(self, trade):
        # 最后清仓
        # for sell_stock in self.current_stocks:
        #     stock_data = self.getdatabyname(sell_stock)
        #     self.close(data=stock_data, exectype=bt.Order.Limit)
        #     logger.debug('最后平仓股票 %s', stock_data._name)

        if not trade.isclosed:
            return
        logger.debug('策略收益：股票[%s], 毛收益 [%.2f], 净收益 [%.2f]', trade.data._name, trade.pnl, trade.pnlcomm)

        # self.__print_broker()


# # 自定义数据
# class FactorData(PandasData):
#     lines = ('market_value', 'momentum', 'peg', 'clv',)
#     params = (('market_value', 7), ('momentum', 8), ('peg', 9), ('clv', 10),)


# 按照backtrader要求的数据格式做数据整理,格式化成backtrader要求：索引日期；列名叫vol和datetime
def comply_backtrader_data_format(df):
    df['trade_date'] = pd.to_datetime(df['trade_date'], format="%Y%m%d")
    df = df.rename(columns={'vol': 'volume', 'trade_date': 'datetime'})  # 列名准从backtrader的命名规范
    df['openinterest'] = 0
    df = df.set_index('datetime')
    df = df.sort_index(ascending=True)
    return df


def main(start_date, end_date, index_code, period, stock_num):
    """
    datetime    open    high    low     close   volume  openi..
    2016-06-24	0.16	0.002	0.085	0.078	0.173	0.214
    2016-06-27	0.16	0.003	0.063	0.048	0.180	0.202
    2016-06-28	0.13	0.010	0.059	0.034	0.111	0.122
    2016-06-29	0.06	0.019	0.058	0.049	0.042	0.053
    2016-06-30	0.03	0.012	0.037	0.027	0.010	0.077
    """

    cerebro = bt.Cerebro()  # 初始化cerebro

    stock_codes = tushare_dbutils.index_weight(index_code, start_date)
    stock_codes = stock_codes[:stock_num]

    combined_factor = factor_combiner.synthesize_by_jaqs(stock_codes, start_date, end_date)
    combined_factor.index = pd.to_datetime(combined_factor.index, format="%Y%m%d")
    logger.debug("合成的多因子为：\n%r", combined_factor)

    d_start_date = utils.str2date(start_date)  # 开始日期
    d_end_date = utils.str2date(end_date)  # 结束日期

    # 加载上证指数，就是为了当日期占位符,在Cerebro中添加上证股指数据,格式: datetime,open,high,low,close,volume,openi..
    df_index = tushare_dbutils.index_daily("000001.SH", start_date, end_date)
    df_index = comply_backtrader_data_format(df_index)
    data = PandasData(dataname=df_index, fromdate=d_start_date, todate=d_end_date)#, plot=False)
    cerebro.adddata(data, name="000001.SH")
    logger.debug("初始化上证数据到脑波：%d 条", len(df_index))

    # 想脑波cerebro逐个追加每只股票的数据
    for stock_code in stock_codes:
        df_stock = tushare_dbutils.daily(stock_code, start_date, end_date)

        trade_days = tushare_dbutils.trade_cal(start_date, end_date)
        if len(df_stock) / len(trade_days) < 0.9:
            logger.warning("股票[%s] 缺失交易日[%d/总%d]天，超过10%%，忽略此股票",
                           stock_code, len(df_stock), len(trade_days))
            continue

        df_stock = comply_backtrader_data_format(df_stock)
        data = PandasData(dataname=df_stock, fromdate=d_start_date, todate=d_end_date)#, plot=False)
        cerebro.adddata(data, name=stock_code)
        logger.debug("初始化股票[%s]数据到脑波cerebro：%d 条", stock_code, len(df_stock))
    logger.debug("合计追加 %d 只股票数据到脑波cerebro", len(stock_codes) + 1)

    ################## cerebro 整体设置 #####################

    # 设置启动资金
    start_cash = 100000.0
    cerebro.broker.setcash(start_cash)

    # https://baike.baidu.com/item/%E8%82%A1%E7%A5%A8%E4%BA%A4%E6%98%93%E6%89%8B%E7%BB%AD%E8%B4%B9/9080806
    # 设置交易手续费：印花税0.001+证管费0.00002+证券交易经手费0.0000487+券商交易佣金0.003
    cerebro.broker.setcommission(commission=0.004)

    # 设置订单份额,设置每笔交易交易的股票数量,比如" cerebro.addsizer(bt.sizers.FixedSize, stake=10)"
    # 告诉平台，我们是每次买卖股票数量固定的，stake=10就是10股。
    # 实际过程中，我们不可能如此简单的制定买卖的数目，而是要根据一定的规则，这就需要自己写一个sizers
    # cerebro.addsizer(Percent)

    # 将交易策略加载到回测系统中
    cerebro.addstrategy(CombineFactorStrategy, index, period, len(df_index), combined_factor)

    # 添加分析对象
    cerebro.addanalyzer(btay.SharpeRatio, _name="sharpe")  # ,timeframe=bt.TimeFrame.Days)  # 夏普指数
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')  # 回撤分析

    # 打印
    logger.debug('回测期间：%r ~ %r , 初始资金: %r', start_date, end_date, start_cash)
    # 运行回测
    results = cerebro.run()
    # 打印最后结果
    portvalue = cerebro.broker.getvalue()
    pnl = portvalue - start_cash
    # 打印结果
    logger.debug("=" * 80)
    logger.debug("股票数: %d 只", len(stock_codes))
    logger.debug("投资期: %s~%s, %d 天", start_date, end_date, (d_end_date - d_start_date).days)
    logger.debug('总资金: %.2f', portvalue)
    logger.debug('余头寸: %.2f', cerebro.broker.getcash())
    logger.debug('净收益: %.2f', pnl)
    logger.debug('收益率: %.2f%%', pnl / portvalue * 100)
    # logger.debug("夏普比: %.2f%%", results[0].analyzers.sharpe.get_analysis()['sharperatio'])
    logger.debug("回撤:   %.2f%%", results[0].analyzers.DW.get_analysis().drawdown)
    cerebro.plot(style="candlestick")#, volume=False)  # 绘图
    # bt.AutoOrderedDict


# python -m example.testback
if __name__ == '__main__':
    start_time = time.time()
    start_date = "20150101"  # 开始日期
    end_date = "20151201"  # 结束日期
    index = '000905.SH'  # 股票池为中证500
    period = 22  # 调仓周期
    stock_num = 10  # 用股票池中的几只，初期调试设置小10，后期可以调成全部
    main(start_date, end_date, index, period, stock_num)
    logger.debug("共耗时: %.0f 秒", time.time() - start_time)
