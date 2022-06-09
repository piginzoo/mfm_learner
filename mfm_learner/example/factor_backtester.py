import argparse
import logging
import time

import backtrader as bt  # 引入backtrader框架
import backtrader.analyzers as bta  # 添加分析函数

from mfm_learner.example.backtest.analyzers.statistics import show_stat
from mfm_learner.example.backtest.analyzers.winrate_analyzer import WinRateAnalyzer
from mfm_learner.example.backtest.strategies.strategy_multifactors import MultiFactorStrategy
from mfm_learner.example.backtest.strategies.strategy_singlefactor import SingleFactorStrategy
from mfm_learner.datasource import datasource_factory, datasource_utils
from mfm_learner.example import factor_utils
from mfm_learner.example.backtest import data_loader
from mfm_learner.example.backtest.analyzers.rebalance_analyzer import RebalanceAnalyzer
from mfm_learner.utils import utils

"""
用factor_tester.py中合成的多因子，做选择股票的策略 ，去选择中证500的股票，跑收益率回测。使用backtrader来做回测框架。
参考：
- https://zhuanlan.zhihu.com/p/351751730
"""

logger = logging.getLogger(__name__)

datasource = datasource_factory.get()


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


# 按照backtrader要求的数据格式做数据整理,格式化成backtrader要求：索引日期；列名叫vol和datetime
def comply_backtrader_data_format(df):
    df = df.rename(columns={'vol': 'volume'})  # 列名准从backtrader的命名规范
    df['openinterest'] = 0  # backtrader需要这列，所以给他补上
    df = datasource_utils.reset_index(df, date_only=True, date_format="%Y-%m-%d")  # 只设置日期列为索引
    df = df.sort_index(ascending=True)
    return df


def __get_strategy_and_factor(factor_names, stock_codes, start_date, end_date):
    # 采用多因子合成
    if factor_names.startswith("synthesis"):
        _, synth_factor_name = factor_names.split(":")
        logger.debug("合成的多因子选股策略：%r", factor_names)
        return SingleFactorStrategy, \
               {synth_factor_name: factor_utils.get_factor_synthesis(synth_factor_name,
                                                                     stock_codes,
                                                                     start_date,
                                                                     end_date)}, \
               "因子合成"

    # 只有一个因子
    if len(factor_names) == 1:
        logger.debug("单因子选股策略: %r", factor_names)
        return SingleFactorStrategy, factor_utils.get_factor_dict(factor_names,
                                                                  stock_codes,
                                                                  start_date,
                                                                  end_date),
        "单因子"

    # 多因子投票
    factor_names = factor_names.split(",")
    logger.debug("多因子共同作用选股策略：%r", factor_names)
    return MultiFactorStrategy, factor_utils.get_factor_dict(factor_names, stock_codes, start_date, end_date), "因子投票"

    raise ValueError("无效的因子选股策略：" + factor_policy)


def main(start_date, end_date, index_code, period, stock_num, factor_names, risk, atr_period, atr_times):
    """
    datetime    open    high    low     close   volume  openi..
    2016-06-24	0.16	0.002	0.085	0.078	0.173	0.214
    2016-06-27	0.16	0.003	0.063	0.048	0.180	0.202
    2016-06-28	0.13	0.010	0.059	0.034	0.111	0.122
    2016-06-29	0.06	0.019	0.058	0.049	0.042	0.053
    2016-06-30	0.03	0.012	0.037	0.027	0.010	0.077
    """
    cerebro = bt.Cerebro()  # 初始化cerebro

    stock_codes = datasource.index_weight(index_code, start_date, end_date)
    stock_codes = stock_codes[:stock_num]

    # 加载指数数据到脑波
    df_benchmark_index = data_loader.load_index_data(cerebro, index_code, start_date, end_date)

    # 加载股票数据到脑波
    data_loader.load_stock_data(cerebro, start_date, end_date, stock_codes, atr_period)

    # 设置启动资金
    start_cash = 500000.0
    cerebro.broker.setcash(start_cash)

    # https://baike.baidu.com/item/%E8%82%A1%E7%A5%A8%E4%BA%A4%E6%98%93%E6%89%8B%E7%BB%AD%E8%B4%B9/9080806
    # 设置交易手续费：印花税0.001+证管费0.00002+证券交易经手费0.0000487+券商交易佣金0.0003
    cerebro.broker.setcommission(commission=0.004)

    # 设置订单份额,设置每笔交易交易的股票数量,比如" cerebro.addsizer(bt.sizers.FixedSize, stake=10)"
    # 告诉平台，我们是每次买卖股票数量固定的，stake=10就是10股。
    # 实际过程中，我们不可能如此简单的制定买卖的数目，而是要根据一定的规则，这就需要自己写一个sizers
    # cerebro.addsizer(Percent)
    strategy_class, factor_dict, name = __get_strategy_and_factor(factor_names=factor_names,
                                                                  stock_codes=stock_codes,
                                                                  start_date=start_date,
                                                                  end_date=end_date)

    # 将交易策略加载到回测系统中，使用addstrategy支持自定义参数，使用optstrategy就不行了，靠！
    # cerebro.addstrategy(strategy_class, period, factor_data)
    # 不用上面的，只能加一个，这里我们加多个调仓期支持（periods）
    cerebro.addstrategy(strategy_class, period=period, factor_dict=factor_dict, atr_times=atr_times, risk=risk)

    # 1年期国债 2%： https://www.cbirc.gov.cn/cn/view/pages/index/guozhai.html

    # 添加分析对象
    cerebro.addanalyzer(bta.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days)  # 夏普指数
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')  # 回撤分析
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.Calmar, _name='calmar') # 卡玛比率 - Calmar：超额收益➗最大回撤
    cerebro.addanalyzer(bt.analyzers.PeriodStats, _name='period_stats')
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='annual')
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')  # 加入PyFolio分析者,这个是为了做quantstats分析用
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade')
    cerebro.addanalyzer(RebalanceAnalyzer, _name='rebalance')
    cerebro.addanalyzer(WinRateAnalyzer, _name='winrate')

    # 打印
    logger.debug('回测期间：%r ~ %r , 初始资金: %r', start_date, end_date, start_cash)
    # 运行回测
    results = cerebro.run(optreturn=True)

    show_stat(cerebro, results, stock_codes, factor_names,
              start_cash, start_date, end_date, period,
              df_benchmark_index,atr_period, atr_times)


"""
# 测试用
python -m mfm_learner.example.factor_backtester \
    --factor momentum_3m \
    --start 20180101 \
    --end 20191230 \
    --num 50 \
    --period 20 \
    --index 000905.SH \
    --risk
    
python -m mfm_learner.example.factor_backtester \
    --factor clv \
    --start 20180101 \
    --end 20190101 \
    --index 000905.SH \
    --num 20 \
    --period 20 \
    --risk  \
    --percent 10%   

python -m mfm_learner.example.factor_backtester \
    --factor synthesis:clv_peg_mv \
    --start 20180101 \
    --end 20190101 \
    --num 20 \
    --period 20 \
    --index 000905.SH

python -m mfm_learner.example.factor_backtester \
    --factor clv,peg,mv,roe_ttm,roe_ \
    --start 20180101 \
    --end 20190101 \
    --num 20 \
    --period 20 \
    --index 000905.SH
"""
if __name__ == '__main__':
    utils.init_logger()

    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--factor', type=str, help="单个因子名、多个（逗号分割）、所有（all）")
    parser.add_argument('-s', '--start', type=str, help="开始日期")
    parser.add_argument('-e', '--end', type=str, help="结束日期")
    parser.add_argument('-i', '--index', type=str, help="股票池code")
    parser.add_argument('-p', '--period', type=int, help="调仓周期，多个的话，用逗号分隔")
    parser.add_argument('-an', '--atr_n', type=int, default=3, help="ATR风控倍数")
    parser.add_argument('-ap', '--atr_p', type=int, default=15, help="ATR周期")
    parser.add_argument('-n', '--num', type=int, help="股票数量")
    parser.add_argument('-r', '--risk', action='store_true', help="是否风控")
    args = parser.parse_args()

    main(args.start,
         args.end,
         args.index,
         args.period,
         args.num,
         args.factor,
         args.risk,
         args.atr_p,
         args.atr_n)
    logger.debug("共耗时: %.0f 秒", time.time() - start_time)
