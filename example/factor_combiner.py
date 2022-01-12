import logging
import os

import numpy as np

from utils import utils

utils.init_logger()

from datasource import datasource_factory
from example import factor_utils
from example.factors.clv import CLVFactor
from example.factors.market_value import MarketValueFactor
from example.factors.momentum import MomentumFactor
from example.factors.peg import PEGFactor

from temp import multifactor_synthesize
import matplotlib
import pandas as pd
import tushare as ts
from alphalens.tears import create_information_tear_sheet, create_returns_tear_sheet
from alphalens.utils import get_clean_factor_and_forward_returns
from jaqs_fxdayu.research.signaldigger import multi_factor

logger = logging.getLogger(__name__)

datasource = datasource_factory.create()

FACTORS = {
    'market_value': MarketValueFactor(),
    "momentum": MomentumFactor(),
    "peg": PEGFactor(),
    "clv": CLVFactor()
}
FACTORS_LONG_SHORT = [-1, 1, 1, 1]  # 因子的多空性质


def get_factors(name, stock_codes, start_date, end_date):
    """
    获得所有的单因子，目前是4个，在FACTORS中定义
    :param name:
    :param stock_codes:
    :param start_date:
    :param end_date:
    :return:
    """
    if name in FACTORS:
        factors = FACTORS[name].calculate(stock_codes, start_date, end_date)
    else:
        raise ValueError("无法识别的因子名称：" + name)
    if not os.path.exists("data/factors"): os.makedirs("data/factors")
    factor_path = os.path.join("data/factors", name + ".csv")
    factors.to_csv(factor_path)
    return factors


def get_stocks(stock_pool, start_date, end_date):
    stock_codes = datasource.index_weight(stock_pool, start_date)
    assert stock_codes is not None and len(stock_codes) > 0, stock_codes
    stock_codes = stock_codes[:stock_num]
    if type(stock_codes) == np.ndarray: stock_codes = stock_codes.tolist()
    logger.debug("从股票池[%s]获得%s~%s %d 只股票用于计算", stock_pool, start_date, end_date, len(stock_codes))
    return stock_codes


def test_by_alphalens(factor_name, stock_pool, start_date, end_date, adjustment_days, stock_num):
    """
    用AlphaLens有个细节，就是你要防止未来函数，

    第二个输入变量是股票的价格数据，它是一个二维数据表(DataFrame)，行是时间，列是股票代码。
    第一是输入的价格数据必须是正确的， 必须是按照信号发出进行回测的，否则会产生前视偏差(lookahead bias)或者使用 到“未来函数”，
    可以加一个缓冲窗口递延交易来解决。例如，通常按照收盘价的回测其实就包含了这样的前视偏差，所以递延到第二天开盘价回测。
    """
    stock_codes = get_stocks(stock_pool, start_date, end_date)
    factors = get_factors(factor_name, stock_codes, start_date, end_date)

    factors = factor_utils.proprocess(factors)

    # 此接口获取的数据为未复权数据，回测建议使用复权数据，这里为批量获取股票数据做了简化
    logger.debug("股票池：%r", stock_codes)
    df = ts.pro_api().daily(ts_code=",".join(stock_codes), start_date=start_date, end_date=end_date)
    df.sort_index(inplace=True)
    # 多索引的因子列，第一个索引为日期，第二个索引为股票代码
    assets = df.set_index([df.index, df['ts_code']], drop=True)
    # column为股票代码，index为日期，值为收盘价
    close = df.pivot_table(index='trade_date', columns='ts_code', values='close')
    close.index = pd.to_datetime(close.index)

    """
    这个是数据规整，
    factors - 必须是 index=[日期|股票], factor value
    prices - 行情数据，一般都是收盘价，index=[日期]，列是所有的股票
    groups - 行业归属数据，就是每天、每只股票隶属哪个行业：index=[日期], 列是：[股票，它归属的行业代码]
    
    """
    factor_data = get_clean_factor_and_forward_returns(factors, prices=close, periods=adjustment_days)

    # Alphalens 有一个特别强大的功能叫 tears 模块，它会生成一张很大的表图，
    # 里面是一张张被称之为撕页(tear sheet)的图片，记录所有与回测相关的 结果
    # create_full_tear_sheet(factor_data, long_short=False)

    long_short = True
    group_neutral = False
    by_group = False
    # plotting.plot_quantile_statistics_table(factor_data)
    factor_returns, mean_quant_ret, mean_quant_rateret, std_quantile, \
    mean_quant_ret_bydate, std_quant_daily, mean_quant_rateret_bydate, \
    compstd_quant_daily, alpha_beta, mean_ret_spread_quant, std_spread_quant \
        = \
        create_returns_tear_sheet(factor_data, long_short, group_neutral, by_group, set_context=False)

    print("factor_returns",factor_returns)
    print("mean_quant_ret",mean_quant_ret)
    print("mean_quant_rateret",mean_quant_rateret)
    print("std_quantile",std_quantile)
    print("mean_quant_ret_bydate",mean_quant_ret_bydate)
    print("std_quant_daily",std_quant_daily)
    print("mean_quant_rateret_bydate",mean_quant_rateret_bydate)
    print("compstd_quant_daily",compstd_quant_daily)
    print("alpha_beta",alpha_beta)
    print("mean_ret_spread_quant",mean_ret_spread_quant)
    print("std_spread_quant",std_spread_quant)

    exit()

    ic_data, (t_values, p_value, skew, kurtosis) = create_information_tear_sheet(factor_data, group_neutral, by_group,
                                                                               set_context=False)
    print("ic_data:", ic_data)
    print("t_stat:", t_values) # 这个是IC们的均值是不是0的检验T值
    # print("p_value:", p_value)
    # print("skew:", skew)
    # print("kurtosis:", kurtosis)
    score(ic_data,t_values)

    # create_turnover_tear_sheet(factor_data, set_context=False)


def score(ic_data,t_values):
    """
    给这个因子一个打分，参考：https://github.com/Jensenberg/multi-factor
    - IC≠0的T检验的t值>2的比例要大于60%（因为是横截面检验，存在50只股票，所以要看着50只的）
    - 因子的偏度要 <0.05的比例大于60%，接近于正态（因为是横截面检验，存在50只股票，所以要看着50只的）
    - TODO 信息系数 ？？？
    - TODO 换手率

    IC是横截面上的50只股票对应的因子值和10天后的收益率的Rank相关性，
    之前理解成1只股票和它的N个10天后的收益率的相关性了，靠，基本概念都错了

    参考：
    https://zhuanlan.zhihu.com/p/24616859

    """

    # 测试ic（因子和股票收益率相关性）
    test_df = pd.DataFrame()

    # 1.看IC≠0的T检验，是根据IC均值不为0
    """         1D          5D          10D         20D
    t_stat: [ 0.15414273  0.44857945 -1.91258305 -5.01587993]
    """
    t_values = np.array(t_values)
    t_flag = np.abs(t_values)>2 # T绝对值大于2，说明

    # 2.看IR是不是大于0.02
    ir_data = ic_data.apply(lambda df: np.abs(df.mean() / df.std()))
    ir_flag = ir_data>0.02

    # print(t_flag,ir_flag)

    """
    3. 看收益率
    3.1 看分组的收益率是不是可以完美分开
    3.2 top组和bottom组的收益率的均值的差值的平均值
    3.3 累计所有股票的收益率（所有的股票的累计收益率的平均值）
    """




def synthesize(stock_pool, start_date, end_date):
    """测试因子合成"""
    stock_codes = get_stocks(stock_pool, start_date, end_date)
    factors = {}
    for factor_key in FACTORS.keys():
        factors[factor_key] = get_factors(factor_key, stock_codes, start_date, end_date)
    logger.debug("开始合成因子：%r", factors.keys())
    combined_factor = multifactor_synthesize.synthesize(factors, None)
    return combined_factor


def synthesize_by_jaqs(stock_codes, start_date, end_date):
    """
    测试因子合成，要求数据得是panel格式的，[trade_date,stock1,stock2,....]
    """
    factor_dict = {}
    for i, factor_key in enumerate(FACTORS.keys()):
        factors = get_factors(factor_key, stock_codes, start_date, end_date)
        factors *= FACTORS_LONG_SHORT[i]  # 空方因子*(-1)
        factor_dict[factor_key] = factor_utils.to_panel_of_stock_columns(factors)

    logger.debug("开始合成因子：%r , 条数：%r",
                 list(factor_dict.keys()),
                 ",".join([str(len(x)) for x in list(factor_dict.values())]))

    df_stocks = datasource.daily(list(stock_codes), start_date, end_date)
    df_stocks = factor_utils.reset_index(df_stocks)
    # unstack将行转化成列
    __prices = df_stocks['close'].unstack()
    __highs = df_stocks['high'].unstack()
    __lows = df_stocks['low'].unstack()

    zz500 = datasource.index_daily('000905.SH', start_date, end_date)
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

    comb_factor = multi_factor.combine_factors(factor_dict,
                                               standardize_type="z_score",
                                               winsorization=False,
                                               weighted_method="ic_weight",
                                               props=props)

    return comb_factor


# python -m example.factor_combiner
if __name__ == '__main__':
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
    matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

    # 参数设置
    start = "20190101"
    end = "20201201"
    adjustment_days = [1, 5, 10, 20, 40, 60]
    stock_pool = '000905.SH'  # 中证500
    stock_num = 50  # 用股票池中的几只，初期调试设置小10，后期可以调成全部

    start = "20200101"
    end = "20201201"
    adjustment_days = [1, 5, 10]
    stock_pool = '000905.SH'  # 中证500
    stock_num = 10  # 用股票池中的几只，初期调试设置小10，后期可以调成全部


    # 测试单因子
    # test_by_alphalens("clv", stock_pool, start, end, adjustment_days, stock_num)
    # test_by_alphalens("momentum", stock_pool, start, end, adjustment_days, stock_num)
    # test_by_alphalens("market_value", stock_pool, start, end, adjustment_days, stock_num)
    test_by_alphalens("peg", stock_pool, start, end, adjustment_days, stock_num)

    # 测试多因子合成
    # combinefactor = synthesize(stock_pool, start, end)

    # 测试JAQS多因子合成
    # stock_codes = get_stocks(stock_pool, start, end)
    # combinefactor = synthesize_by_jaqs(stock_codes, start, end)
    # logger.debug("合成因子：")
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    #     print(combinefactor)  # .dropna(how="all").head())
