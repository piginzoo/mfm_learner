import argparse
import logging
import time

import backtrader as bt  # 引入backtrader框架
import backtrader.analyzers as bta  # 添加分析函数
import quantstats as qs
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo

from datasource import datasource_factory, datasource_utils
from example import factor_utils
from example.backtest import data_loader
from example.backtest.strategy_multifactors import MultiFactorStrategy
from example.backtest.strategy_singlefactor import SingleFactorStrategy
from utils import utils

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
    df = datasource_utils.reset_index(df, date_only=True)  # 只设置日期列为索引
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


def main(start_date, end_date, index_code, period, stock_num, factor_names, factor_policy, risk, atr_period, atr_times):
    """
    datetime    open    high    low     close   volume  openi..
    2016-06-24	0.16	0.002	0.085	0.078	0.173	0.214
    2016-06-27	0.16	0.003	0.063	0.048	0.180	0.202
    2016-06-28	0.13	0.010	0.059	0.034	0.111	0.122
    2016-06-29	0.06	0.019	0.058	0.049	0.042	0.053
    2016-06-30	0.03	0.012	0.037	0.027	0.010	0.077
    """
    cerebro = bt.Cerebro()  # 初始化cerebro

    d_start_date = utils.str2date(start_date)
    d_end_date = utils.str2date(end_date)

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

    # 将交易策略加载到回测系统中
    # cerebro.addstrategy(strategy_class, period, factor_data)
    # 不用上面的，只能加一个，这里我们加多个调仓期支持（periods）
    cerebro.addstrategy(strategy_class, period=period, factor_dict=factor_dict, atr_times=atr_times, risk=risk)

    # 添加分析对象
    cerebro.addanalyzer(bta.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days)  # 夏普指数
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')  # 回撤分析
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.PeriodStats, _name='period_stats')
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='annual')
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')  # 加入PyFolio分析者,这个是为了做quantstats分析用

    # 打印
    logger.debug('回测期间：%r ~ %r , 初始资金: %r', start_date, end_date, start_cash)
    # 运行回测
    results = cerebro.run(optreturn=True)
    # 打印最后结果
    portvalue = cerebro.broker.getvalue()
    pnl = portvalue - start_cash

    for i, result in enumerate(results):

        # 打印结果
        logger.debug("-" * 80)
        logger.debug("调仓周期：%d 天" % period)
        logger.debug("股票个数: %d 只", len(stock_codes))
        logger.debug("投资期间: %s~%s, %d 天", start_date, end_date, (d_end_date - d_start_date).days)
        logger.debug("因子策略: %s", factor_policy)
        logger.debug('期初投资: %.2f', start_cash)
        logger.debug('期末总额: %.2f', portvalue)
        logger.debug('剩余现金: %.2f', cerebro.broker.getcash())
        logger.debug('持仓头寸: %.2f', portvalue - cerebro.broker.getcash())
        logger.debug('净收益额: %.2f', pnl)
        logger.debug('收益率  : %.2f%%', pnl / portvalue * 100)
        logger.debug("夏普比  : %.2f%%", result.analyzers.sharpe.get_analysis()['sharperatio'] * 100)
        logger.debug("回撤    : %.2f%%", result.analyzers.DW.get_analysis().drawdown)
        logger.debug("总收益  : %.2f%%", result.analyzers.returns.get_analysis()['rtot'] * 100)
        logger.debug("年化收益: %.2f%%", result.analyzers.returns.get_analysis()['ravg'] * 100)
        logger.debug("平均收益: %.2f%%", result.analyzers.returns.get_analysis()['rnorm100'])
        logger.debug("期间统计    : %r", result.analyzers.period_stats.get_analysis())
        logger.debug("年化:")
        for year, year_return in result.analyzers.annual.get_analysis().items():
            logger.debug("\t %s : %.2f%%", year, year_return * 100)
        # cerebro.plot(plotter=MyPlot(), style="candlestick", iplot=False)
        quant_statistics(df_benchmark_index['close'], result, period, name, factor_names, atr_period, atr_times)

    b = Bokeh(stype='bar', tabs="multi", scheme=Tradimo())
    cerebro.plot(b)


def quant_statistics(df_benchmark_index, strat, period, name, factor_names, atr_p, atr_n):
    portfolio_stats = strat.analyzers.getbyname('PyFolio')  # 得到PyFolio分析者实例

    # 以下returns为以日期为索引的资产日收益率系列
    returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()

    returns.index = returns.index.tz_convert(None)  # 索引的时区要设置一下，否则出错

    # 输出html策略报告,rf为无风险利率
    qs.reports.html(returns,
                    benchmark=df_benchmark_index,
                    output='debug/回测报告_{}_{}天调仓_{}.html'.format(utils.today(), period, name),
                    title='{}日调仓,{},因子:{},ATR:{}天/{}倍'.format(period, name, factor_names, atr_p, atr_n), rf=0.0)

    # print(qs.reports.metrics(returns=returns, mode='full'))
    # df = qs.reports.metrics(returns=returns, mode='full', display=False)
    # print("返回的QuantStats报表：\n%r", df)
    # qs.reports.basic(returns)


"""
# 测试用
python -m example.factor_backtester \
    --factor clv \
    --start 20180101 \
    --end 20191230 \
    --num 50 \
    --period 20 \
    --index 000905.SH \
    --risk
    
python -m example.factor_backtester \
    --factor clv \
    --start 20180101 \
    --end 20190101 \
    --index 000905.SH \
    --num 20 \
    --period 20 \
    --risk  \
    --percent 10%   

python -m example.factor_backtester \
    --factor synthesis:clv_peg_mv \
    --start 20180101 \
    --end 20190101 \
    --num 20 \
    --period 20 \
    --index 000905.SH

python -m example.factor_backtester \
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
    parser.add_argument('-t', '--type', type=str, help="合成还是分开使用：synthesis|separated")
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
         args.type,
         args.risk,
         args.atr_p,
         args.atr_n)
    logger.debug("共耗时: %.0f 秒", time.time() - start_time)
