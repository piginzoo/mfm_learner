import logging
from mfm_learner import utils
utils.init_logger()

import matplotlib
import pandas as pd
import tushare
import tushare as ts
import market_value_factor
from alphalens.tears import create_returns_tear_sheet, create_information_tear_sheet, create_turnover_tear_sheet, \
    create_full_tear_sheet
from alphalens.tears import plotting
from alphalens.utils import get_clean_factor_and_forward_returns



logger = logging.getLogger(__name__)


def main(stock_pool, start, end, stock_num):
    """
    用AlphaLens有个细节，就是你要防止未来函数，

    第二个输入变量是股票的价格数据，它是一个二维数据表(DataFrame)，行是时间，列是股票代码。
    第一是输入的价格数据必须是正确的， 必须是按照信号发出进行回测的，否则会产生前视偏差(lookahead bias)或者使用 到“未来函数”，
    可以加一个缓冲窗口递延交易来解决。例如，通常按照收盘价的回测其实就包含了这样的前视偏差，所以递延到第二天开盘价回测。
    """
    universe = market_value_factor.get_universe(stock_pool, start, stock_num)
    assert len(universe) > 0, str(len(universe))

    """
    第一个输入变量是股票的因子值，它是一个序列(Series)并且具有多重索引(Mult iIndex)，
    该多重索引的两个索引分别是日期(date)和股票代码(asset)，而且日期的索引层级(level 0)优先级高于股票代码的索引层级(level 1)。
    Series 的第三列(前两列是索引)才是股票的因子值，如苹果公司 (AAPL)的因子值是 0.5。
    实现多重索引的方法是 df.set_index([„date‟,‟asset‟])。
    """
    factors = market_value_factor.LNCAP(universe=universe, start=start, end=end)
    factors = market_value_factor.preprocess(factors)
    factors.index = pd.to_datetime(factors.index) # 时间为日期格式，tushare是str
    factors = factors.unstack() # 将LNCAP的因子格式从列为股票名，转变成，日期+股票的联合索引，注意，unstack导致股票名称在前
    factors.index.names=['ts_code','trade_date']
    factors = factors.reorder_levels(['trade_date','ts_code'])

    # 此接口获取的数据为未复权数据，回测建议使用复权数据，这里为批量获取股票数据做了简化
    logger.debug("股票池：%r",universe)
    df = ts.pro_api().daily(ts_code=",".join(universe.tolist()), start_date=start, end_date=end)
    df.sort_index(inplace=True)
    # 多索引的因子列，第一个索引为日期，第二个索引为股票代码
    assets = df.set_index([df.index, df['ts_code']], drop=True)
    # column为股票代码，index为日期，值为收盘价
    close = df.pivot_table(index='trade_date', columns='ts_code', values='close')
    close.index = pd.to_datetime(close.index)

    factor_data = get_clean_factor_and_forward_returns(factors, close)

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



# python -m alphalen.market_value_factor_alpha_lens
if __name__ == '__main__':
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
    matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题
    conf = utils.load_config()
    tushare.set_token(conf['tushare']['token'])

    start = "20210101"
    end = "20211201"
    day_window = 5
    stock_pool = '000300.SH'
    stock_num = 10  # 用股票池中的几只，初期调试设置小10，后期可以调成全部

    main(stock_pool,start,end,stock_num)