import logging
import math
from abc import abstractmethod

import backtrader as bt  # 引入backtrader框架
import numpy as np

from utils import utils

logger = logging.getLogger(__name__)


class MultiStocksFactorStrategy(bt.Strategy):
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

    @abstractmethod
    def do_next(self):
        pass

    def next(self):
        self.do_next()

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
