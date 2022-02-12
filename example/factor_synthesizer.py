import argparse
import logging
import time

import matplotlib
from jaqs_fxdayu.research.signaldigger import multi_factor

from datasource import datasource_factory, datasource_utils
from example import factor_utils
from temp import multifactor_synthesize
from utils import utils

logger = logging.getLogger(__name__)

datasource = datasource_factory.create()

"""
合成多因子，保存到数据库

合成因子的表结构：
- datetime
- code
- value
- factors
- weight
表明：
factor_synth_clv_mv_ic_weight
"""


def synthesize(stock_codes, start_date, end_date):
    """测试因子合成"""
    factors = {}
    for factor_key in factor_utils.FACTORS.keys():
        factors[factor_key] = factor_utils.get_factor(factor_key, stock_codes, start_date, end_date)

    logger.debug("开始合成因子：%r", factors.keys())
    combined_factor = multifactor_synthesize.synthesize(factors, None)
    return combined_factor


def synthesize_by_jaqs(stock_codes, factor_dict, start_date, end_date, weight="ic_weight"):
    """
    测试因子合成，要求数据得是panel格式的，[trade_date,stock1,stock2,....]
    """
    logger.debug("开始合成因子：%r , 条数：%r",
                 list(factor_dict.keys()),
                 ",".join([str(len(x)) for x in list(factor_dict.values())]))

    df_stocks = datasource.daily(list(stock_codes), start_date, end_date)
    df_stocks = datasource_utils.reset_index(df_stocks)
    # unstack将行转化成列
    __prices = df_stocks['close'].unstack()
    __highs = df_stocks['high'].unstack()
    __lows = df_stocks['low'].unstack()

    zz500 = datasource.index_daily('000905.SH', start_date, end_date)
    zz500 = datasource_utils.reset_index(zz500)
    zz500 = zz500['close'].pct_change(1)
    zz500 = factor_utils.to_panel_of_stock_columns(zz500)
    assert len(zz500) != 0

    logger.debug("close价格：%d 条,索引：%r", len(__prices), list(__prices.index.names))
    logger.debug("high价格：%d 条,索引：%r", len(__highs), __highs.index.names)
    logger.debug("low价格：%d 条,索引：%r", len(__lows), __lows.index.names)
    logger.debug("中证价格：%d 条,索引：%r", len(zz500), zz500.index.names)

    props = {
        'price': __prices,
        'high': __highs,  # 可为空
        'low': __lows,  # 可为空
        'ret_type': 'return',  # 可选参数还有upside_ret/downside_ret 则组合因子将以优化潜在上行、下行空间为目标
        'daily_benchmark_ret': zz500,  # 为空计算的是绝对收益　不为空计算相对收益
        'period': 10,  # 30天的持有期
        'forward': True,
        'commission': 0.0008,
        "covariance_type": "shrink",  # 协方差矩阵估算方法 还可以为"simple"
        "rollback_period": 10}  # 滚动窗口天数

    """按照IC_Weight进行合成"""
    comb_factor = multi_factor.combine_factors(factor_dict,
                                               standardize_type="z_score",
                                               winsorization=False,
                                               weighted_method=weight,
                                               props=props)

    return comb_factor


def main(start_date, end_date, index_code, stock_num, factor_names, weight):
    start_time = time.time()
    stock_codes = datasource.index_weight(index_code, start_date, end_date)
    stock_codes = stock_codes[:stock_num]

    factor_dict = {}
    for factor_name in factor_names:
        df_factor = factor_utils.get_factor(stock_codes, None, start, end)
        factor_dict[factor_name] = df_factor
    df_combined_factor = synthesize_by_jaqs(stock_codes, factor_dict, start, end, weight)

    print(df_combined_factor.head(3))

    df_combined_factor = df_combined_factor.reset_index()
    synth_facor_name = "synth_" + "_".join(factor_names) + "_" + weight
    factor_utils.factor2db(name=synth_facor_name, factor=df_combined_factor)

    logger.info("合成因子[%s] %d行， 耗时 %.2f 秒", factor_names, len(df_combined_factor), time.time() - start_time)


"""
python -m example.factor_synthesizer \
    --factor clv,peg,mv \
    --start 20170101 \
    --end 20180101 \
    --num 20 \
    --index 000905.SH
"""

if __name__ == '__main__':
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
    matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

    utils.init_logger()

    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--factor', type=str, help="单个因子名、多个（逗号分割）、所有（all）")
    parser.add_argument('-t', '--weight', type=str, help="合成还是分开使用：synthesis|separated")
    parser.add_argument('-s', '--start', type=str, help="开始日期")
    parser.add_argument('-e', '--end', type=str, help="结束日期")
    parser.add_argument('-i', '--index', type=str, help="股票池code")
    parser.add_argument('-p', '--period', type=str, help="调仓周期，多个的话，用逗号分隔")
    parser.add_argument('-n', '--num', type=int, help="股票数量")
    args = parser.parse_args()

    if "," in args.period:
        periods = [int(p) for p in args.period.split(",")]
    else:
        periods = [int(args.period)]

    if "," in args.factor:
        factors = [int(p) for p in args.factor.split(",")]
    else:
        factors = [int(args.factor)]

    main(args.start,
         args.end,
         args.index,
         periods,
         args.num,
         factors,
         args.weight)
    logger.debug("共耗时: %.0f 秒", time.time() - start_time)
