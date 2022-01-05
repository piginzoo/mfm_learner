import logging
import math

import pandas as pd
from backtrader.feeds import PandasData

from example import factor_combiner
from utils import utils

utils.init_logger()
import backtrader as bt  # 引入backtrader框架
import backtrader.analyzers as btay  # 添加分析函数
import numpy as np
from utils import tushare_utils

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
        异常处理：
        - 如果股票停盘，会顺序买入下一位排名的股票
        - 每次都是满仓，即用卖出的股票头寸，全部购入新的股票，头寸仅在新购入股票中平均分配
        - 如果没有头寸，则不再购买（这种情况应该不会出现）
        """

        # 回测最后一天不进行买卖,datas[0]就是当天, bugfix:  之前self.datas[0].date(0)不行，因为df.index是datetime类型的
        current_date = self.datas[0].datetime.datetime(0)

        self.current_day += 1
        self.count += 1

        if self.current_day < self.period: return

        self.current_day = 0

        factor = self.factors.loc[current_date]

        logger.debug("交易日：%r , %d/%d", utils.date2str(current_date), self.count, self.total)
        if np.isnan(factor).all():
            logger.debug("%r 日的因子全部为NAN，忽略当日", utils.date2str(current_date))
            return

        factor = factor.dropna()
        factor = factor.sort_values(ascending=False)
        logger.debug("当天的因子为：%r",factor)
        # 选择因子值前20%
        select_stocks = factor.index[:math.ceil(0.2*len(factor))]
        logger.debug("此次选中的股票为：%r",select_stocks)

        # 以往买入的标的，本次不在标的中，则先平仓
        to_sell_stocks = set(self.current_stocks) - set(select_stocks)
        for sell_stock in to_sell_stocks:
            logger.debug('卖出 %s 平仓: %r', sell_stock , self.getposition(sell_stock).size)
            o = self.close(sell_stock)
            self.order_list.append(o)  # 记录订单
            self.current_stocks.remove(sell_stock)
        logger.debug("合计卖出%d只股票，剩余%d只持仓",len(to_sell_stocks),len(self.current_stocks))


        # 每只股票买入资金百分比，预留2%的资金以应付佣金和计算误差
        buy_percentage = (1 - 0.02) / len(select_stocks)

        # 得到目标市值
        targetvalue = buy_percentage * self.broker.getvalue()

        next_trade_day = self.datas[0].datetime(1)  # 下一交易日
        for buy_stock in select_stocks:
            if buy_stock in self.current_stocks:
                logger.debug("%s 在持仓中，不动", buy_stock)
                continue

            # 按次日开盘价计算下单量，下单量是100的整数倍
            size = int(
                abs((self.broker.getvalue([d]) - targetvalue) / d.open[1] // 100 * 100))
            if self.broker.getvalue([d]) > targetvalue:  # 持仓过多，要卖
                # 次日跌停价近似值
                lowerprice = d.close[0] * 0.9 + 0.02

                o = self.sell(data=d, size=size, exectype=bt.Order.Limit,
                              price=lowerprice, valid=validday)
            else:  # 持仓过少，要买
                # 次日涨停价近似值
                upperprice = d.close[0] * 1.1 - 0.02
                o = self.buy(data=d, size=size, exectype=bt.Order.Limit,
                             price=upperprice, valid=validday)

            self.order_list.append(o)  # 记录订单




# 修改原数据加载模块，以便能够加载更多自定义的因子数据
class FactorData(PandasData):
    lines = ('market_value', 'momentum', 'peg', 'clv',)
    params = (('market_value', 7), ('momentum', 8), ('peg', 9), ('clv', 10),)


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

    # 加载指数，就是为了当日期占位符
    df_index = tushare_utils.index_daily("000001.SH", start_date, end_date)

    stock_codes = tushare_utils.index_weight(index_code, start_date)
    stock_codes = stock_codes[:stock_num]

    combined_factor = factor_combiner.synthesize_by_jaqs(stock_codes, start_date, end_date)
    combined_factor.index = pd.to_datetime(combined_factor.index, format="%Y%m%d")
    logger.debug("合成的多因子为：\n%r",combined_factor)

    start_date = utils.str2date(start_date)  # 开始日期
    end_date = utils.str2date(end_date)  # 结束日期

    # 格式化成backtrader要求：索引日期；列名叫vol和datetime
    df_index['trade_date'] = pd.to_datetime(df_index['trade_date'], format="%Y%m%d")
    df_index = df_index.rename(columns={'vol': 'volume', 'trade_date': 'datetime'})
    df_index = df_index.set_index('datetime')

    data = PandasData(dataname=df_index, fromdate=start_date, todate=end_date)
    # 在Cerebro中添加股票数据
    cerebro.adddata(data, name="000001.SH")

    ################## cerebro整体设置 #####################

    # 设置启动资金
    startcash = 100000.0
    cerebro.broker.setcash(startcash)

    # https://baike.baidu.com/item/%E8%82%A1%E7%A5%A8%E4%BA%A4%E6%98%93%E6%89%8B%E7%BB%AD%E8%B4%B9/9080806
    # 设置交易手续费：印花税0.001+证管费0.00002+证券交易经手费0.0000487+券商交易佣金0.003
    cerebro.broker.setcommission(commission=0.004)

    # 设置订单份额,设置每笔交易交易的股票数量,比如" cerebro.addsizer(bt.sizers.FixedSize, stake=10)"
    # 告诉平台，我们是每次买卖股票数量固定的，stake=10就是10股。
    # 实际过程中，我们不可能如此简单的制定买卖的数目，而是要根据一定的规则，这就需要自己写一个sizers
    cerebro.addsizer(Percent)

    # 将交易策略加载到回测系统中
    cerebro.addstrategy(CombineFactorStrategy, index, period, len(df_index), combined_factor)

    # 添加分析对象
    cerebro.addanalyzer(btay.SharpeRatio, _name="sharpe")  # 夏普指数
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')  # 回撤分析

    # 打印
    logger.debug('回测期间：%r ~ %r , 初始资金: %r', start_date, end_date, startcash)
    # 运行回测
    results = cerebro.run()
    # 打印最后结果
    portvalue = cerebro.broker.getvalue()
    pnl = portvalue - startcash
    # 打印结果
    logger.debug(f'总资金: {round(portvalue, 2)}')
    logger.debug(f'净收益: {round(pnl, 2)}')
    logger.debug("夏普比例:", results[0].analyzers.sharpe.get_analysis())
    logger.debug("回撤", results[0].analyzers.DW.get_analysis())

    # cerebro.plot(style="candlestick")  # 绘图


# python -m example.testback
if __name__ == '__main__':
    start = "20200101"  # 开始日期
    end = "20201201"  # 结束日期
    index = '000905.SH'  # 股票池为中证500
    period = 20  # 调仓周期
    stock_num = 10  # 用股票池中的几只，初期调试设置小10，后期可以调成全部
    main(start, end, index, period, stock_num)
