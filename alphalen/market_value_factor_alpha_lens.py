import matplotlib
import pandas as pd
import tushare
import tushare as ts
import market_value_factor
from alphalens.tears import create_returns_tear_sheet, create_information_tear_sheet,create_turnover_tear_sheet
from alphalens.tears import plotting
from alphalens.utils import get_clean_factor_and_forward_returns
import utils



def main():
    """
    用AlphaLens有个细节，就是你要防止未来函数，

    第二个输入变量是股票的价格数据，它是一个二维数据表(DataFrame)，行是时间，列是股票代码。
    第一是输入的价格数据必须是正确的， 必须是按照信号发出进行回测的，否则会产生前视偏差(lookahead bias)或者使用 到“未来函数”，
    可以加一个缓冲窗口递延交易来解决。例如，通常按照收盘价的回测其实就包含了这样的前视偏差，所以递延到第二天开盘价回测。
    """
    universe = market_value_factor.get_universe(stock_pool, start, stock_num)
    assert len(universe) > 0, str(len(universe))

    # 市值因子，按照日期
    factors = market_value_factor.LNCAP(universe=universe, start=start, end=end)
    factors = market_value_factor.proprocess(factors)

    market_value_factor.getForwardReturns()

    # 此接口获取的数据为未复权数据，回测建议使用复权数据，这里为批量获取股票数据做了简化
    df = ts.pro_api().daily(ts_code='000001.SZ,600982.SH', start_date='20200101', end_date='20211122')
    df.index = pd.to_datetime(df['trade_date'])
    df.index.name = None
    df.sort_index(inplace=True)

    # 多索引的因子列，第一个索引为日期，第二个索引为股票代码
    assets = df.set_index([df.index, df['ts_code']], drop=True)

    # column为股票代码，index为日期，值为收盘价
    close = df.pivot_table(index='trade_date', columns='ts_code', values='close')
    close.index = pd.to_datetime(close.index)

    # 我们是使用pct_chg因子数据预测收盘价，因此需要偏移1天，但是这里有2只股票，所以是shift(2)
    # 将 因子数据、价格数据以及行业分类按照索引对齐地格式化到一个数据表中，这个数据表的索引是包含日期和资产的多重索引”，
    # 我们理解就是获取清洗后的 因子及其 未来收益(可以包含行业，也可以不包含行业)，并将它们的收益对齐
    factor_data = get_clean_factor_and_forward_returns(assets[['pct_chg']].shift(2), close)

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
    utils.init_logger()
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
    matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题
    conf = utils.load_config()
    tushare.set_token(conf['tushare']['token'])

    start = "20210101"
    end = "20211201"
    day_window = 5
    stock_pool = '000300.SH'
    stock_num = 50  # 用股票池中的几只，初期调试设置小10，后期可以调成全部

    main(start,end)