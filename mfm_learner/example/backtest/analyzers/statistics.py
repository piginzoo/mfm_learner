import logging

import quantstats as qs
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo

from mfm_learner.utils import utils

logger = logging.getLogger(__name__)


def show_stat(cerebro, results, stock_codes, factor_names,
              start_cash, start_date, end_date, period,
              df_benchmark_index, atr_period, atr_times):
    broker = cerebro.broker
    d_start_date = utils.str2date(start_date)
    d_end_date = utils.str2date(end_date)
    portvalue = broker.getvalue()
    pnl = portvalue - start_cash

    for i, result in enumerate(results):

        # 打印结果
        logger.debug("-" * 80)
        logger.debug("调仓周期：%d 天" % period)
        logger.debug("股票个数: %d 只", len(stock_codes))
        logger.debug("投资期间: %s~%s, %d 天", start_date, end_date, (d_end_date - d_start_date).days)
        logger.debug("因子策略: %s", factor_names)
        logger.debug('期初投资: %.2f', start_cash)
        logger.debug('期末总额: %.2f', portvalue)
        logger.debug('剩余现金: %.2f', broker.getcash())
        logger.debug('持仓头寸: %.2f', portvalue - broker.getcash())
        logger.debug('净收益额: %.2f', pnl)
        logger.debug('收益率  : %.2f%%', pnl / portvalue * 100)
        logger.debug("夏普比  : %.2f%%", result.analyzers.sharpe.get_analysis()['sharperatio'] * 100)
        logger.debug("回撤    : %.2f%%", result.analyzers.DW.get_analysis().drawdown)

        logger.debug("总收益  : %.2f%%", result.analyzers.returns.get_analysis()['rtot'] * 100)
        logger.debug("年化收益: %.2f%%", result.analyzers.returns.get_analysis()['ravg'] * 100)
        logger.debug("平均收益: %.2f%%", result.analyzers.returns.get_analysis()['rnorm100'])

        logger.debug("每年收益   : %.2f%%", result.analyzers.period_stats.get_analysis()['average'])
        logger.debug("年收益方差  : %.2f%%", result.analyzers.period_stats.get_analysis()['stddev'])
        logger.debug("正收益年数  : %d年", result.analyzers.period_stats.get_analysis()['positive'])
        logger.debug("负收益年数  : %d年", result.analyzers.period_stats.get_analysis()['negative'])
        logger.debug("最好的收益  : %.2f%%", result.analyzers.period_stats.get_analysis()['best'])
        logger.debug("最差的收益  : %.2f%%", result.analyzers.period_stats.get_analysis()['worst'])

        logger.debug("换仓率      : %r", result.analyzers.rebalance.get_analysis()['rebalance_rate'])


        logger.debug("年化:")
        for year, year_return in result.analyzers.annual.get_analysis().items():
            logger.debug("\t %s : %.2f%%", year, year_return * 100)
        print_trade_detail_stat(result.analyzers.trade.get_analysis())

        logger.debug("日胜数pk基准: %d", result.analyzers.winrate.get_analysis()['win_day'])
        logger.debug("日败数pk基准: %d", result.analyzers.winrate.get_analysis()['fail_day'])
        logger.debug("日胜率pk基准: %.1f%%", result.analyzers.winrate.get_analysis()['win_rate_day']*100)
        logger.debug("月胜数pk基准: %d", result.analyzers.winrate.get_analysis()['win_month'])
        logger.debug("月败数pk基准: %d", result.analyzers.winrate.get_analysis()['fail_month'])
        logger.debug("月胜率pk基准: %.1f%%", result.analyzers.winrate.get_analysis()['win_rate_month'] * 100)
        logger.debug("年胜数pk基准: %d", result.analyzers.winrate.get_analysis()['win_year'])
        logger.debug("年胜数pk基准: %d", result.analyzers.winrate.get_analysis()['fail_year'])
        logger.debug("年胜数pk基准: %.1f%%", result.analyzers.winrate.get_analysis()['win_rate_year']*100)

        logger.debug("日胜数    : %d", result.analyzers.winrate.get_analysis()['positive_day'])
        logger.debug("日败数    : %d", result.analyzers.winrate.get_analysis()['negative_day'])
        logger.debug("日胜率    : %.1f%%", result.analyzers.winrate.get_analysis()['pnl_rate_day']*100)
        logger.debug("月胜数    : %d", result.analyzers.winrate.get_analysis()['positive_month'])
        logger.debug("月败数    : %d", result.analyzers.winrate.get_analysis()['negative_month'])
        logger.debug("月胜率    : %.1f%%", result.analyzers.winrate.get_analysis()['pnl_rate_month'] * 100)
        logger.debug("年胜数    : %d", result.analyzers.winrate.get_analysis()['positive_year'])
        logger.debug("年胜数    : %d", result.analyzers.winrate.get_analysis()['negative_year'])
        logger.debug("年胜率    : %.1f%%", result.analyzers.winrate.get_analysis()['pnl_rate_year']*100)


        # cerebro.plot(plotter=MyPlot(), style="candlestick", iplot=False)
        quant_statistics(df_benchmark_index['close'], result, period, factor_names, atr_period,
                         atr_times)


    b = Bokeh(stype='bar', tabs="multi", scheme=Tradimo())
    cerebro.plot(b)


def quant_statistics(df_benchmark_index, strat, period, factor_names, atr_p, atr_n):
    portfolio_stats = strat.analyzers.getbyname('PyFolio')  # 得到PyFolio分析者实例

    # 以下returns为以日期为索引的资产日收益率系列
    returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()

    returns.index = returns.index.tz_convert(None)  # 索引的时区要设置一下，否则出错

    # 输出html策略报告,rf为无风险利率
    qs.reports.html(returns,
                    benchmark=df_benchmark_index,
                    output='debug/回测报告_{}_{}天调仓_{}.html'.format(utils.today(), period, factor_names),
                    title='{}日调仓,因子:{},ATR:{}天/{}倍'.format(period, factor_names, atr_p, atr_n),
                    rf=0.0)

    # print(qs.reports.metrics(returns=returns, mode='full'))
    # df = qs.reports.metrics(returns=returns, mode='full', display=False)
    # print("返回的QuantStats报表：\n%r", df)
    # qs.reports.basic(returns)


def print_trade_detail_stat(trade):
    """
    TradeAnalyzer()
    total_total	10              总共几次交易
    total_open	1               未完成的交易
    total_closed	9           完成的交易
    streak_won_current	0
    streak_won_longest	3       最长连续赢：3次
    streak_lost_current	1
    streak_lost_longest	1       最长连续输：1次
    pnl_gross_total	23.16975        收益率 23%
    pnl_gross_average	2.574416667 平均收益率：2.6%
    pnl_net_total	23.16975        净收益率：
    pnl_net_average	2.574416667     平均净收益率：
    won_total	6                   赢次数
    won_pnl_total	42.99625        赢的合计收益
    won_pnl_average	7.166041667     赢的平均收益
    won_pnl_max	16.645              赢的最大单次收益
    lost_total	3                   输次数
    lost_pnl_total	-19.8265        输总负收益
    lost_pnl_average	-6.608833333输平均负收益 PNL = Positive Negative Loss
    ============================================================================================================
    Provides statistics on closed trades (keeps also the count of open ones)
    - Total Open/Closed Trades
    - Streak Won/Lost Current/Longest
    - ProfitAndLoss Total/Average
    - Won/Lost Count/ Total PNL/ Average PNL / Max PNL
    - Long/Short Count/ Total PNL / Average PNL / Max PNL
        - Won/Lost Count/ Total PNL/ Average PNL / Max PNL
    - Length (bars in the market)
        - Total/Average/Max/Min
        - Won/Lost Total/Average/Max/Min
        - Long/Short Total/Average/Max/Min
          - Won/Lost Total/Average/Max/Min
    ============================================================================================================
    total       总交易数
     total 28
     open 1     未完成的
     closed 27  完成的
    --------------------------------------------------------------------------------
    streak      连续输赢情况
     won
         current 0  ？？？
         longest 2  最长的连续赢
     lost
         current 1  ？？？
         longest 8  最长的连续输
    --------------------------------------------------------------------------------
    pnl                             PNL: Profit and Loss
     gross
         total -100.8000000000001   毛利
         average -3.733333333333337 平均毛利
     net
         total -100.8000000000001   净收益
         average -3.733333333333337 平均每单净收益
    --------------------------------------------------------------------------------
    won                             赢利次数
     total 8
     pnl
         total 195.19999999999996   总盈利
         average 24.399999999999995 平均盈利
         max 92.59999999999998      最大盈利
    --------------------------------------------------------------------------------
    lost                            总失败
     total 19
     pnl
         total -296.0000000000001    总损失
         average -15.578947368421058 平均损失
         max -35.60000000000002      最大损失
    --------------------------------------------------------------------------------
    long
     total 27
     pnl
         total -100.8000000000001
         average -3.733333333333337
         won
             total 195.19999999999996
             average 24.399999999999995
             max 92.59999999999998
         lost
             total -296.0000000000001
             average -15.578947368421058
             max -35.60000000000002
     won 8
     lost 19
    --------------------------------------------------------------------------------
    short
     total 0
     pnl
         total 0.0
         average 0.0
         won
             total 0.0
             average 0.0
             max 0.0
         lost
             total 0.0
             average 0.0
             max 0.0
     won 0
     lost 0
    --------------------------------------------------------------------------------
    len
     total 128
     average 4.7407407407407405
     max 14
     min 1
     won
         total 74
         average 9.25
         max 14
         min 2
     lost
         total 54
         average 2.8421052631578947
         max 8
         min 1
     long
         total 128
         average 4.7407407407407405
         max 14
         min 1
         won
             total 74
             average 9.25
             max 14
             min 2
         lost
             total 54
             average 2.8421052631578947
             max 8
             min 1
     short
         total 0
         average 0.0
         max 0
         min 9223372036854775807
         won
             total 0
             average 0.0
             max 0
             min 9223372036854775807
         lost
             total 0
             average 0.0
             max 0
             min 9223372036854775807
    """
    logger.debug("总交易数：%d", trade['total']['total'])
    logger.debug("完成交易：%d", trade['total']['open'])
    logger.debug("未完交易：%d", trade['total']['closed'])

    logger.debug("总净收益：%.2f", trade['pnl']['net']['total'])
    logger.debug("单净收益：%.2f", trade['pnl']['net']['average'])

    logger.debug("赚钱次数：%d", trade['won']['total'])
    logger.debug("赚钱总额：%.2f", trade['won']['pnl']['total'])
    logger.debug("赚钱每单：%.2f", trade['won']['pnl']['average'])
    logger.debug("赚钱单最：%.2f", trade['won']['pnl']['max'])

    logger.debug("亏钱次数：%d", trade['lost']['total'])
    logger.debug("亏钱总额：%.2f", trade['lost']['pnl']['total'])
    logger.debug("亏钱每单：%.2f", trade['lost']['pnl']['average'])
    logger.debug("亏钱单最：%.2f", trade['lost']['pnl']['max'])
