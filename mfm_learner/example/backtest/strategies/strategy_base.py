import logging
import math
from abc import abstractmethod

from mfm_learner.example.backtest.risk.risk_control import RiskControl
from mfm_learner.example.backtest.trade_recorder import TradeRecorder
from mfm_learner.utils import utils

utils.init_logger()

import backtrader as bt  # 引入backtrader框架
import numpy as np

logger = logging.getLogger(__name__)


#
#
# class TradeResult():
#     """
#     date
#     List<code    sell/buy    profit>
#     """
#     pass
#     def trade(self,codes,buy_or_sells, profits):


class MultiStocksFactorStrategy(bt.Strategy):
    """
    多个股票的回测的父类，是一个**基类**，
    有个抽象方法，叫selected_stocks(current_date)用来选取当期股票
    """

    def __init__(self, period, factor_dict, atr_times, risk):
        self.period = period
        self.stop_flag = False
        self.factor_dict = factor_dict
        self.current_date = ''
        self.rebalance_rates = []

        self.risk = risk
        self.risk_control = RiskControl(self, atr_times, period)
        logger.debug("因子选股：因子[%r], ATR倍数[%d], 调仓周期[%d]天", ",".join(list(factor_dict.keys())), atr_times, period)

        self.trade_recorder = TradeRecorder()

        self.trade_listeners = []
        self.trade_listeners.append(self.trade_recorder)
        self.trade_listeners.append(self.risk_control)

    def __post_trade(self, trade):
        for l in self.trade_listeners:
            l.on_trade(trade)

    def __print_broker(self):
        # logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~")
        # logger.debug('|  当前总资产:%.2f', self.broker.getvalue())
        # logger.debug('|  当前总头寸:%.2f', self.broker.getcash())
        # logger.debug('|  当前持仓量:%.2f', self.broker.getposition(self.data).size)
        # logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~")
        pass

    @abstractmethod
    def sort_stocks(self, factors, current_date):
        pass

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

    # 这个是一只股票的一个完整交易的生命周期：开仓，持有，卖出
    def notify_trade(self, trade):
        # 最后清仓
        # for sell_stock in self.trade_recorder.get(:
        #     stock_data = self.getdatabyname(sell_stock)
        #     self.close(data=stock_data, exectype=bt.Order.Limit)
        #     logger.debug('最后平仓股票 %s', stock_data._name)

        if trade.isclosed:
            open_date = utils.date2str(bt.num2date(trade.dtopen))
            close_date = utils.date2str(bt.num2date(trade.dtopen))
            logger.debug('策略收益：股票[%s], 毛收益 [%.2f], 净收益 [%.2f],交易开始日期[%s]~[%s]',
                         trade.data._name, trade.pnl, trade.pnlcomm,
                         open_date, close_date)

        self.__post_trade(trade)

        # self.__print_broker()

    def sell_out(self, stock_code):
        # 根据名字获得对应那只股票的数据
        stock_data = self.getdatabyname(stock_code)

        # size = self.getsizing(stock_data,isbuy=False)
        # self.sell(data=stock_data,exectype=bt.Order.Limit,size=size)
        size = self.getposition(stock_data, self.broker).size
        self.close(data=stock_data, exectype=bt.Order.Limit)

        logger.debug('[%s] 平仓股票 %s : 卖出%r股', self.current_date, stock_data._name, size)

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

        # 风控
        blacklist_stocks = []
        if self.risk:
            blacklist_stocks = self.risk_control.execute()

        # 调用self.env.stoprun()后，还会run next方法，加个flag控制异常
        if self.stop_flag: return

        # logger.debug("已经处理了%d个数据, 总共有%d个数据", len(self), self.data.buflen())

        # 回测最后一天不进行买卖,datas[0]就是当天, bugfix:  之前self.datas[0].date(0)不行，因为df.index是datetime类型的
        current_date = self.datas[0].datetime.datetime(0)
        self.current_date = utils.date2str(current_date)

        if not len(self.data) % self.period == 0: return

        logger.debug("-" * 50)
        logger.debug("第%d个交易日：%r ", len(self.data), utils.date2str(current_date))

        # 因子对股票排序
        selected_stocks = self.sort_stocks(self.factor_dict, current_date)

        # 选择topN的
        selected_stocks = self._select_top_n(selected_stocks, blacklist_stocks)
        if selected_stocks is None: return
        if type(selected_stocks) == np.array: selected_stocks = selected_stocks.tolist()
        logger.debug("此次选中的股票为：%r", ",".join(selected_stocks))

        # 以往买入的标的，本次不在标的中，则先平仓
        # "常规下单函数主要有 3 个：买入 buy() 、卖出 sell()、平仓 close() "
        to_sell_stocks = set(self.trade_recorder.get_stocks()) - set(selected_stocks)

        # 计算调仓率
        if len(self.trade_recorder.get_stocks()) > 0:
            rebalance_rate = len(to_sell_stocks) / len(self.trade_recorder.get_stocks())
            self.rebalance_rates.append(rebalance_rate)

        # 1. 清仓未在选择列表的股票
        logger.debug("[%s] 卖出股票：%r", self.current_date, to_sell_stocks)
        for sell_stock in to_sell_stocks:
            self.sell_out(sell_stock)

        logger.debug("[%s] 卖出%d只股票，剩余%d只持仓", self.current_date, len(to_sell_stocks),
                     len(self.trade_recorder.get_stocks()))

        self.__print_broker()

        # 每只股票买入资金百分比，预留2%的资金以应付佣金和计算误差
        buy_percentage = (1 - 0.02) / len(selected_stocks)

        # 得到可以用来购买的金额
        buy_amount_per_stock = buy_percentage * self.broker.getcash()

        # 2. 买入选择的股票
        for buy_stock in selected_stocks:
            self._buy_in(buy_stock, buy_amount_per_stock)

        self.__print_broker()

    def _buy_in(self, stock_code, buy_amount):
        # 防止股票不在数据集中
        if stock_code not in self.getdatanames():
            logger.warning("[%s] 股票[%s]不在数据集中", self.current_date, stock_code)
            return

        # 如果选中的股票在当前的持仓中，就忽略
        if stock_code in self.trade_recorder.get_stocks():
            logger.debug("[%s] %s 在持仓中，不动", self.current_date, stock_code)
            return

        # 根据名字获得对应那只股票的数据
        stock_data = self.getdatabyname(stock_code)

        # 买，用当天的开盘价，假设的场景是，你决定一早就买了，就按当天的开盘价买
        open_price = stock_data.open[0]

        # TODO：按次日开盘价计算下单量，下单量是100（手）的整数倍 ？？？次日价格，还是，本日价格？
        size = math.ceil(buy_amount / open_price)
        logger.debug("[%s] 购入股票[%s 股价%.2f] %d股，金额:%.2f", self.current_date, stock_code, open_price, size, buy_amount)
        self.buy(data=stock_data, size=size, price=open_price, exectype=bt.Order.Limit)

    def _select_top_n(self, stock_codes, blacklist_stocks):
        """
        剔除在黑名单上的股票后，选择前1/5的股票，
        短期没有其他类似需求，不做重构
        :param stock_codes:
        :return:
        """
        stock_codes = [stock_code for stock_code in stock_codes if stock_code not in blacklist_stocks]

        # 选择因子值前20%
        top_n = math.ceil(0.2 * len(stock_codes))
        select_stocks = stock_codes[:top_n]

        logger.debug("[%s] 此次选中的股票为：%r", self.current_date, ",".join(select_stocks))
        return select_stocks
