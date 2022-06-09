import argparse
import logging
import time

import matplotlib
import pandas as pd
from jaqs_fxdayu.research.signaldigger import multi_factor

from mfm_learner.datasource import datasource_factory, datasource_utils
from mfm_learner.example import factor_utils
from mfm_learner.example.factor_utils import to_panel_of_stock_columns
from mfm_learner.utils import utils

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


def synthesize_by_jaqs(stock_codes, factor_dict, df_index_daily, start_date, end_date, weight="ic_weight"):
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

    logger.debug("close价格：%d 条,索引：%r", len(__prices), list(__prices.index.names))
    logger.debug("high价格：%d 条,索引：%r", len(__highs), __highs.index.names)
    logger.debug("low价格：%d 条,索引：%r", len(__lows), __lows.index.names)
    logger.debug("指数价格：%d 条,索引：%r", len(df_index_daily), df_index_daily.index.names)

    props = {
        'price': __prices,
        'high': __highs,  # 可为空
        'low': __lows,  # 可为空
        'ret_type': 'return',  # 可选参数还有upside_ret/downside_ret 则组合因子将以优化潜在上行、下行空间为目标
        'daily_benchmark_ret': df_index_daily,  # 为空计算的是绝对收益　不为空计算相对收益
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


def main(name, desc, start_date, end_date, index_code, stock_num, factor_names, weight):
    start_time = time.time()
    stock_codes = datasource.index_weight(index_code, start_date, end_date)
    stock_codes = stock_codes[:stock_num]

    factor_dict = {}
    for factor_name in factor_names:
        df_factor = factor_utils.get_factor(factor_name, stock_codes, start_date, end_date)
        # jaqs_fxdayu要求的因子格式必须是，每行是一个日期，
        # 所以要从[日期|股票]+值的Series，转换成，[日期|股票1|股票2|...|股票n]的panel数据
        factor_dict[factor_name] = to_panel_of_stock_columns(df_factor)

    df_index_daily = datasource.index_daily(index_code, start_date, end_date)
    df_index_daily = datasource_utils.reset_index(df_index_daily)
    df_index_daily = df_index_daily['close'].pct_change(1)
    df_index_daily = factor_utils.to_panel_of_stock_columns(df_index_daily)
    assert len(df_index_daily) != 0

    df_combined_factor = synthesize_by_jaqs(stock_codes, factor_dict, df_index_daily, start_date, end_date, weight)

    logger.debug("合成后的因子（3行）：\n%r", df_combined_factor.head(3))

    df_combined_factor = df_combined_factor.stack()  # 把合成因子，包含多只股票的列，通过stack()，把股票列，变成行
    df_combined_factor = pd.DataFrame(df_combined_factor)  # 要转成Dataframe，才可以改列名
    df_combined_factor.columns = ['value'] # 合成的值的列名
    df_combined_factor = df_combined_factor.reset_index()  # 保存到数据库中，索引变成普通列
    factor_utils.factor_synthesis2db(name=name, desc=desc, df_factor=df_combined_factor)

    logger.info("合成因子[%s] %d行， 耗时 %.2f 秒", factor_names, len(df_combined_factor), time.time() - start_time)


"""
python -m mfm_learner.example.factor_synthesizer \
    --name 'clv_peg_mv' \
    --desc 'clv,peg,mv' \
    --factor clv,peg,mv \
    --start 20180101 \
    --end 20190101 \
    --num 20 \
    --weight ic_weight \
    --index 000905.SH
"""

if __name__ == '__main__':
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
    matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

    utils.init_logger()

    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--name', type=str, help="名字，需要唯一")
    parser.add_argument('-d', '--desc', type=str, default="", help="描述")
    parser.add_argument('-f', '--factor', type=str, help="单个因子名、多个（逗号分割）、所有（all）")
    parser.add_argument('-t', '--weight', type=str, help="合成还是分开使用：synthesis|separated")
    parser.add_argument('-s', '--start', type=str, help="开始日期")
    parser.add_argument('-e', '--end', type=str, help="结束日期")
    parser.add_argument('-i', '--index', type=str, help="股票池code")
    parser.add_argument('-n', '--num', type=int, help="股票数量")
    args = parser.parse_args()

    if "," not in args.factor: raise ValueError("请提供多个因子，逗号分割 : " + args.factor)

    factors = [p for p in args.factor.split(",")]

    main(
        args.name,
        args.desc,
        args.start,
        args.end,
        args.index,
        args.num,
        factors,
        args.weight)
    logger.debug("共耗时: %.0f 秒", time.time() - start_time)
