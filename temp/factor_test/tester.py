"""
自己实现一个因子测试的，思路上，还是标准的那套，之前也写过，alphalens也有，但是，
要自己再实现一遍，而且，作为今后自己的使用的主要手段。
"""
from pandas import Series

from example import factor_utils

FACTORS = {
    'market_value': market_value,
    "momentum": momentum,
    "peg": peg,
    "clv": clv
}

CONFIG = {
    'valid_percent': 0.9,
    'fill_nan': 'mean',  # no 不填充| mean 均值 | median 中位数 | follow 跟随日期的最后一个值,
    'winsorize_quantile': [0.025, 0.975],  # 值的缩尾程度，去掉两端的极端值
}


def process(factor):
    # 因子格式必须是 Index[code,datetime]的Series或者1列的DataFrame
    # 这个是跟着alphalens学的
    # 因子包含三项: 日期 + 股票 + 因子值
    assert list(factor.index.names) == ['code', 'datetime']
    assert type(factor) == Series or len(factor.columns) == 1

    # 去极值，找出这只股票的所有在两侧的极端值，并用分位数的值进行填充
    factor = factor.groupby(level='code').apply(factor_utils.winsorize, CONFIG['winsorize_quantile'])

    # 填充INF值成NAN
    factor = factor.apply(factor_utils.fill_inf)

    # 处理NAN
    factor = factor.groupby(level='code').apply(factor_utils.fill_nan, CONFIG['fill_nan'])

    # 标准化z-score
    factor = factor.groupby(level='code').apply(factor_utils.zscore)
