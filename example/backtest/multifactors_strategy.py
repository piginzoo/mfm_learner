import logging
import math

import backtrader as bt  # 引入backtrader框架
import numpy as np
import pandas as pd

from utils import utils

logger = logging.getLogger(__name__)


class MultiFactorStrategy(bt.Strategy):
    """
    和旁边那个CombineFactorStategy不同，这个策略不需要做多因子合成，而是用每一个因子都参与进来，一起打分，
    然后用每个因子的打分的总分，给每支股票评价，然后选出调仓的股票。
    当然实盘的时候需要每次都要计算，回测的好处就是，可以提前都计算好这些有因子值。
    所以，我只需要把每期的因子值拿出来，然后对股票进行排序就可以了。
    注意，每支股票还得老老实实的传入进来，原因是要用第二天的open价格信息作为买入价格呢。
    TODO：现在因子数据是作为数据传入的（是一个外部数据，没有用到backtrader的lines数据格式），
    TODO：然后每个换仓周期，再按照日期+股票去获对应因子取值排序的，那，为何不最开始的时候，直接将计算完的因子值合并到股票的数据中呢？
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
        self.factors = factors  # 这个是一个字典，key是因子名，value是因子值
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
        - 找出此期间中证500只包含的股票池中的股票，TODO：这里有个坑，股票池是变动的，最简单的办法是获得回测期间整个中证500的股票并集
        - 根据每支股票的当日的数据，计算每一个单因子值，然后对因子值进行排序，排序序号，作为这只股票在这个因子上的得分
        - 然后合并这只股票的所有的因子得分，得到一个总分，
        - 根据总分进行排序，得到总排序，然后找出最好的100只备选股
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

        # 如果不到换仓日，忽略
        if self.current_day < self.period: return

        logger.debug("-" * 50)

        self.current_day = 0

        select_stocks = self.select_stocks_by_score(self.factors,current_date)

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

    def select_stocks_by_score(self, factors, current_date):
        df_stock_scores = []

        # 遍历每一个因子(因子是 index:[datetime,code], columns:factor_value)
        for name, factor in factors.items():
            # 得到当天的因子
            factor = factor.loc[current_date]
            # 按照value排序，reset_index()会自动生成从0开始索引，用这点来生成排序序号，酷
            df_sorted_by_factor_values = factor.sort_values().reset_index()
            # 再利用reset_index，生成排序列
            df_stock_rank_by_factor = df_sorted_by_factor_values.reset_index()
            df_stock_rank_by_factor.columns = ['index', 'code']
            # 把索引换成股票代码
            df_stock_rank_by_factor = df_stock_rank_by_factor.set_index('code')
            df_stock_scores.append(df_stock_rank_by_factor)

        df_stock_scores = pd.concat(df_stock_scores, axis=1)

        df_stock_scores[:, 'score'] = df_stock_scores.sum(axis=1)

        logger.debug("交易日：%r , %d/%d", utils.date2str(current_date), self.count, self.total)
        if np.isnan(factor).all():
            logger.debug("%r 日的因子全部为NAN，忽略当日", utils.date2str(current_date))
            return None

        df_stock_scores = df_stock_scores.dropna()
        # logger.debug("当天的因子为：%r", factor)
        # 选择因子值前20%
        select_stocks = df_stock_scores.index[:math.ceil(0.2 * len(factor))]
        logger.debug("此次选中的股票为：%r", ",".join(select_stocks.tolist()))
        return select_stocks
