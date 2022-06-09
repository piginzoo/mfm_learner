import logging

from mfm_learner.example.backtest.strategies.strategy_base import MultiStocksFactorStrategy
from mfm_learner.utils import utils

logger = logging.getLogger(__name__)


class SingleFactorStrategy(MultiStocksFactorStrategy):
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

    def sort_stocks(self, factors, current_date):
        """
        factors,对于singlefactor子类，他应该是只有1个factor的dict，
        factor是一个DataFrame，
        index[datetime,code],value是因子值，

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

        assert type(factors) == dict and len(factors.keys()) == 1, str(len(factors.keys()))

        # factors是一个dict,factors的values长度为1，内容为DataFrame，index[datetime,code]
        df_factor = list(factors.values())[0]

        assert len(df_factor) > 0, len(df_factor)

        # print(df_factor.head(3))
        # xs函数，是截面函数，只取current_date日的截面数据

        df_cross_sectional = df_factor.xs(current_date)
        df_cross_sectional = df_cross_sectional.dropna()
        # 按照降序排列,只有1列，即因子值
        df_cross_sectional = df_cross_sectional.sort_values(by=df_cross_sectional.columns[0],
                                                            ascending=False)
        # logger.debug("交易日：%r , 第#%d个交易日，因子(3行)：\n%r", utils.date2str(current_date), self.count, df_cross_sectional.head(3))

        if df_cross_sectional.empty:
            logger.waning("%r 日的因子为空，忽略当日", utils.date2str(current_date))
            return None

        return df_cross_sectional.index
