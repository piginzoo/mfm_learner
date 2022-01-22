import logging

import numpy as np
import pandas as pd

from utils import utils

utils.init_logger()
from datasource import datasource_factory, datasource_utils
from example import factor_utils

from temp import multifactor_synthesize
import matplotlib
from jaqs_fxdayu.research.signaldigger import multi_factor

logger = logging.getLogger(__name__)

datasource = datasource_factory.create()


def get_stocks(stock_pool, start_date, end_date):
    stock_codes = datasource.index_weight(stock_pool, start_date)
    assert stock_codes is not None and len(stock_codes) > 0, stock_codes
    stock_codes = stock_codes[:stock_num]
    if type(stock_codes) == np.ndarray: stock_codes = stock_codes.tolist()
    logger.debug("从股票池[%s]获得%s~%s %d 只股票用于计算", stock_pool, start_date, end_date, len(stock_codes))
    return stock_codes


def synthesize(stock_pool, start_date, end_date):
    """测试因子合成"""
    stock_codes = get_stocks(stock_pool, start_date, end_date)
    factors = {}
    for factor_key in factor_utils.FACTORS.keys():
        factors[factor_key] = factor_utils.get_factor(factor_key, stock_codes, start_date, end_date)
    logger.debug("开始合成因子：%r", factors.keys())
    combined_factor = multifactor_synthesize.synthesize(factors, None)
    return combined_factor


def synthesize_by_jaqs(stock_codes, factor_dict, start_date, end_date):
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
                                               weighted_method="ic_weight",
                                               props=props)

    return comb_factor


# python -m example.factor_synthesizer
if __name__ == '__main__':
    pd.set_option('display.max_rows', 1000)
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
    matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

    # 参数设置
    start = "20190101"
    end = "20201201"
    periods = [1, 5, 10, 20, 40, 60]
    stock_pool = '000905.SH'  # 中证500
    stock_num = 50  # 用股票池中的几只，初期调试设置小10，后期可以调成全部

    # 调试用
    start = "20200101"
    end = "20200901"
    periods = [1, 5, 10]
    stock_pool = '000905.SH'  # 中证500
    stock_num = 10  # 用股票池中的几只，初期调试设置小10，后期可以调成全部

    # 测试JAQS多因子合成
    stock_codes = get_stocks(stock_pool, start, end)
    factor_dict = factor_utils.get_factors(stock_codes, None, start, end)
    combinefactor = synthesize_by_jaqs(stock_codes, factor_dict, start, end)
    logger.debug("合成因子：")
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(combinefactor)  # .dropna(how="all").head())
