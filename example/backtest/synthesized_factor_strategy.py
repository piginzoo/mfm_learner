import logging
import math

import backtrader as bt  # 引入backtrader框架
import numpy as np

from example.backtest.multi_stocks_strategy import MultiStocksFactorStrategy
from utils import utils

logger = logging.getLogger(__name__)


class SynthesizedFactorStrategy(MultiStocksFactorStrategy):
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

    def do_next(self):
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
