import logging

import utils
from example import factor_utils

utils.init_logger()
import tushare_utils

import matplotlib
import pandas as pd
import tushare
import tushare as ts
import market_value_factor
from alphalens.tears import create_returns_tear_sheet, create_information_tear_sheet, create_turnover_tear_sheet
from alphalens.tears import plotting
from alphalens.utils import get_clean_factor_and_forward_returns
import numpy as np

logger = logging.getLogger(__name__)
period_window = 5

"""

"""

def load_stock_data(stock_codes, start, end):
    df_merge = None
    for stock_code in stock_codes:
        df_daily = tushare_utils.daily(stock_code=stock_code, start_date=start, end_date=end)
        if df_merge is None:
            df_merge = df_daily
        else:
            df_merge = df_merge.append(df_daily)
        logger.debug("加载%s~%s的股票[%s]的%d条交易和基本信息的合并数据", start, end, stock_code, len(df_merge))
    logger.debug("一共加载%s~%s %d条数据", start, end, len(df_merge))
    return df_merge


def get_factor(stock_codes, start_date, end_date):
    """
    计算动量，动量，就是往前回溯period个周期，然后算收益，
    但是，为了防止有的价格高低，所以用log方法，更接近，参考：https://zhuanlan.zhihu.com/p/96888358
    :param period_window: 
    :param df:
    :return:
    """
    df = load_stock_data(stock_codes, start_date, end_date)
    adj_close = (df['close'] + df['high'] + df['low']) / 3
    df['momentum'] = np.log(adj_close / adj_close.shift(period_window))  # shift(1) 往后移，就变成上个月的了
    df = df[['trade_date', 'ts_code', 'momentum']]
    df['trade_date'] = pd.to_datetime(df['trade_date'], format="%Y%m%d")  # 时间为日期格式，tushare是str
    df = df.set_index(['trade_date', 'ts_code'])
    return df


def get_stock_names(stock_pool, start, stock_num):
    universe = market_value_factor.get_universe(stock_pool, start, stock_num)
    assert len(universe) > 0, str(len(universe))
    return universe


def load_stock_data(stock_codes, start, end):
    df_merge = None
    for stock_code in stock_codes:
        df_daily = tushare_utils.daily(stock_code=stock_code, start_date=start, end_date=end)
        df_basic = tushare_utils.daily_basic(stock_code=stock_code, start_date=start, end_date=end)
        df_basic.info()
        df_basic.drop(['close'], axis=1, inplace=True)  # close 字段重复
        # import pdb; pdb.set_trace()
        df_merge_temp = df_daily.merge(df_basic, on=['ts_code', 'trade_date'], how='left')
        if df_merge is None:
            df_merge = df_merge_temp
        else:
            df_merge = df_merge.append(df_merge_temp)
        logger.debug("加载%s~%s的股票[%s]的%d条交易和基本信息的合并数据", start, end, stock_code, len(df_merge))
    logger.debug("一共加载%s~%s %d条数据", start, end, len(df_merge))
    return df_merge


def main(stock_pool, start_date, end_date, adjustment_days, stock_num):
    """
    用AlphaLens有个细节，就是你要防止未来函数，

    第二个输入变量是股票的价格数据，它是一个二维数据表(DataFrame)，行是时间，列是股票代码。
    第一是输入的价格数据必须是正确的， 必须是按照信号发出进行回测的，否则会产生前视偏差(lookahead bias)或者使用 到“未来函数”，
    可以加一个缓冲窗口递延交易来解决。例如，通常按照收盘价的回测其实就包含了这样的前视偏差，所以递延到第二天开盘价回测。
    """
    stock_codes = tushare_utils.index_weight(stock_pool, start_date)
    assert stock_codes is not None and len(stock_codes) > 0, stock_codes
    stock_codes = stock_codes[:stock_num]
    stock_data = load_stock_data(stock_codes, start_date, end_date)
    logger.debug("获得%s~%s %d 条交易数据", start_date, end_date, len(stock_data))

    """
    第一个输入变量是股票的因子值，它是一个序列(Series)并且具有多重索引(Mult iIndex)，
    该多重索引的两个索引分别是日期(date)和股票代码(asset)，而且日期的索引层级(level 0)优先级高于股票代码的索引层级(level 1)。
    Series 的第三列(前两列是索引)才是股票的因子值，如苹果公司 (AAPL)的因子值是 0.5。
    实现多重索引的方法是 df.set_index([„date‟,‟asset‟])。
    """
    factors = get_factor(period_window=adjustment_days, df=stock_data)
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


# python -m example.factors.momentum
if __name__ == '__main__':
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
    matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题
    conf = utils.load_config()
    tushare.set_token(conf['tushare']['token'])

    start = "20201101"
    end = "20201201"
    adjustment_days = 5
    stock_pool = '000300.SH'
    stock_num = 10  # 用股票池中的几只，初期调试设置小10，后期可以调成全部

    main(stock_pool, start, end, adjustment_days, stock_num)
