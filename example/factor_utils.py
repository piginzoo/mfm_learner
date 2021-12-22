import pandas as pd
import logging

logger = logging.getLogger(__name__)

def __winsorize_series(se):
    """
    把分数为97.5%和2.5%之外的异常值替换成分位数值
    :param se:
    :return:
    """
    q = se.quantile([0.025, 0.975])
    """
    quantile：
        >>> s = pd.Series([1, 2, 3, 4])
        >>> s.quantile([.25, .5, .75])
        0.25    1.75
        0.50    2.50
        0.75    3.25    
    """
    if isinstance(q, pd.Series) and len(q) == 2:
        se[se < q.iloc[0]] = q.iloc[0]
        se[se > q.iloc[1]] = q.iloc[1]
    return se


def __standardize_series(se):
    """标准化"""
    se_std = se.std()
    se_mean = se.mean()
    return (se - se_mean) / se_std


def __fillna_series(se):
    return se.fillna(se.dropna().mean())


def proprocess(factors):
    factors = factors.groupby(level='trade_date').apply(__fillna_series)  # 填充NAN
    factors = factors.groupby(level='trade_date').apply(__winsorize_series)  # 去极值
    factors = factors.groupby(level='trade_date').apply(__standardize_series)  # 标准化
    logger.debug("规范化预处理完市值因子(LNCAP)，%d行", len(factors))
    return factors

def reset_index(factors):
    factors['trade_date'] = pd.to_datetime(factors['trade_date'], format="%Y%m%d")  # 时间为日期格式，tushare是str
    factors = factors.set_index(['trade_date', 'ts_code'])
    return factors