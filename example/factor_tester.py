import logging
import os

import utils

utils.init_logger()
utils.tushare_login()
from example import factor_utils, multifactor_synthesize
from example.factors import momentum, peg, clv, market_value
import tushare_utils
import matplotlib
import pandas as pd
import tushare as ts
from alphalens.tears import create_returns_tear_sheet, create_information_tear_sheet, create_turnover_tear_sheet
from alphalens.tears import plotting
from alphalens.utils import get_clean_factor_and_forward_returns

logger = logging.getLogger(__name__)

FACTORS = {
    'market_value': market_value,
    "momentum": momentum,
    "peg": peg,
    "clv": clv
}


def get_factors(name, stock_codes, start_date, end_date):
    if name in FACTORS:
        factors = FACTORS[name].get_factor(stock_codes, start_date, end_date)
    else:
        raise ValueError("无法识别的因子名称：" + name)
    if not os.path.exists("data/factors"): os.makedirs("data/factors")
    factor_path = os.path.join("data/factors", name + ".csv")
    factors.to_csv(factor_path)
    return factors


def get_stocks(stock_pool, start_date, end_date):
    stock_codes = tushare_utils.index_weight(stock_pool, start_date)
    assert stock_codes is not None and len(stock_codes) > 0, stock_codes
    stock_codes = stock_codes[:stock_num]
    logger.debug("从股票池[%s]获得%s~%s %d 只股票用于计算", stock_pool, start_date, end_date, len(stock_codes))
    return stock_codes


def test(factor_name, stock_pool, start_date, end_date, adjustment_days, stock_num):
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
    df = ts.pro_api().daily(ts_code=",".join(stock_codes.tolist()), start_date=start_date, end_date=end_date)
    df.sort_index(inplace=True)
    # 多索引的因子列，第一个索引为日期，第二个索引为股票代码
    assets = df.set_index([df.index, df['ts_code']], drop=True)
    # column为股票代码，index为日期，值为收盘价
    close = df.pivot_table(index='trade_date', columns='ts_code', values='close')
    close.index = pd.to_datetime(close.index)

    factor_data = get_clean_factor_and_forward_returns(factors, close, periods=[adjustment_days])

    # Alphalens 有一个特别强大的功能叫 tears 模块，它会生成一张很大的表图，
    # 里面是一张张被称之为撕页(tear sheet)的图片，记录所有与回测相关的 结果
    # create_full_tear_sheet(factor_data, long_short=False)
    long_short = True
    group_neutral = False
    by_group = False
    plotting.plot_quantile_statistics_table(factor_data)
    create_returns_tear_sheet(factor_data, long_short, group_neutral, by_group, set_context=False)
    create_information_tear_sheet(factor_data, group_neutral, by_group, set_context=False)
    create_turnover_tear_sheet(factor_data, set_context=False)


def synthesize(stock_pool, start_date, end_date):
    """测试因子合成"""
    stock_codes = get_stocks(stock_pool, start_date, end_date)
    factors = {}
    for factor_key in FACTORS.keys():
        factors[factor_key] = get_factors(factor_key, stock_codes, start_date, end_date)
    logger.debug("开始合成因子：%r",factors.keys())
    combined_factor = multifactor_synthesize.synthesize(factors,None)
    return combined_factor


# python -m example.factor_tester
if __name__ == '__main__':
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
    matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题
    start = "20200101"
    end = "20201201"
    adjustment_days = 5
    stock_pool = '000300.SH'
    stock_num = 10  # 用股票池中的几只，初期调试设置小10，后期可以调成全部

    # 测试单因子
    # test("clv", stock_pool, start, end, adjustment_days, stock_num)
    # test("momentum", stock_pool, start, end, adjustment_days, stock_num)
    # test("market_value", stock_pool, start, end, adjustment_days, stock_num)
    # test("peg", stock_pool, start, end, adjustment_days, stock_num)

    # 测试多因子合成
    combinefactor = synthesize(stock_pool, start, end)
