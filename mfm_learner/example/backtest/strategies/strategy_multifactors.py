import logging

import pandas as pd
from pandas import DataFrame

from mfm_learner.example.backtest.strategies.strategy_base import MultiStocksFactorStrategy

logger = logging.getLogger(__name__)


class MultiFactorStrategy(MultiStocksFactorStrategy):
    """
    和旁边那个CombineFactorStategy不同，这个策略不需要做多因子合成，而是用每一个因子都参与进来，一起打分，
    然后用每个因子的打分的总分，给每支股票评价，然后选出调仓的股票。
    当然实盘的时候需要每次都要计算，回测的好处就是，可以提前都计算好这些有因子值。
    所以，我只需要把每期的因子值拿出来，然后对股票进行排序就可以了。
    注意，每支股票还得老老实实的传入进来，原因是要用第二天的open价格信息作为买入价格呢。
    TODO：现在因子数据是作为数据传入的（是一个外部数据，没有用到backtrader的lines数据格式），
    TODO：然后每个换仓周期，再按照日期+股票去获对应因子取值排序的，那，为何不最开始的时候，直接将计算完的因子值合并到股票的数据中呢？
    """

    def sort_stocks(self, factor_dict, current_date):
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
        logger.debug("已经处理了%d个bar, 总共有%d个bar", len(self), self.data.buflen())

        # logger.debug("--------------------------------------------------")
        # logger.debug('当前可用资金:%r', self.broker.getcash())
        # logger.debug('当前总资产:%r', self.broker.getvalue())
        # logger.debug('当前持仓量:%r', self.broker.getposition(self.data).size)
        # logger.debug('当前持仓成本:%r', self.broker.getposition(self.data).price)
        # logger.debug('当前持仓量:%r', self.getposition(self.data).size)
        # logger.debug('当前持仓成本:%r', self.getposition(self.data).price)
        # logger.debug("--------------------------------------------------")

        select_stocks = self.select_stocks_by_score(factor_dict, current_date)

        return select_stocks

    def select_stocks_by_score(self, factor_dict, current_date):
        df_stock_scores = []

        # 遍历每一个因子(因子是 index:[datetime,code], columns:factor_value)
        for name, factor in factor_dict.items():
            assert current_date in factor.index, str(current_date) + "不在因子[" + name + "]的日期范围内"

            # 得到当天的因子
            factor = factor.loc[current_date]
            # 按照value排序，reset_index()会自动生成从0开始索引，用这点来生成排序序号，酷
            factor = DataFrame(factor)
            assert len(factor.columns) == 1
            df_sorted_by_factor_values = factor.sort_values(by=factor.columns[0]).reset_index()
            # 再利用reset_index，生成排序列
            df_stock_rank_by_factor = df_sorted_by_factor_values.reset_index()
            df_stock_rank_by_factor.columns = ['index', 'code', 'factor_value']
            # 把索引换成股票代码
            df_stock_rank_by_factor = df_stock_rank_by_factor.set_index('code')
            df_stock_scores.append(df_stock_rank_by_factor['index'])

        df_stock_scores = pd.concat(df_stock_scores, axis=1)

        # 按照score列求和
        df_stock_scores.loc[:, 'score'] = df_stock_scores.sum(axis=1)

        df_stock_scores = df_stock_scores.dropna()

        logger.debug("因子排序成绩为：\n%r", df_stock_scores.head(3))
        # 选择因子值前20%
        select_stocks = df_stock_scores.sort_values(by='score', ascending=False).index

        return select_stocks
