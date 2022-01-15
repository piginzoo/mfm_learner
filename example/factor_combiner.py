import logging
import os

import numpy as np
from alphalens import tears
from matplotlib.ticker import ScalarFormatter
from pandas import Series, DataFrame
import matplotlib.cm as cm
from utils import utils
import matplotlib.pyplot as plt

utils.init_logger()
from datasource import datasource_factory, datasource_utils
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


def test_by_alphalens(factor_name, stock_pool, start_date, end_date, periods, stock_num):
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

    df = datasource.daily(stock_codes, start_date=start_date, end_date=end_date)
    df = datasource_utils.reset_index(df)

    # column为股票代码，index为日期，值为收盘价
    close = df.pivot_table(index='datetime', columns='code', values='close')

    """
    这个是数据规整，
    factors - 必须是 index=[日期|股票], factor value
    prices - 行情数据，一般都是收盘价，index=[日期]，列是所有的股票
    groups - 行业归属数据，就是每天、每只股票隶属哪个行业：index=[日期], 列是：[股票，它归属的行业代码]
    
    """
    factor_data = get_clean_factor_and_forward_returns(factors, prices=close, periods=periods)

    # Alphalens 有一个特别强大的功能叫 tears 模块，它会生成一张很大的表图，
    # 里面是一张张被称之为撕页(tear sheet)的图片，记录所有与回测相关的 结果
    # create_full_tear_sheet(factor_data, long_short=False)

    long_short = True
    group_neutral = False
    by_group = False

    # plotting.plot_quantile_statistics_table(factor_data)
    factor_returns, mean_quantile_ret_bydate = \
        create_returns_tear_sheet(factor_data, long_short, group_neutral, by_group, set_context=False,
                                  factor_name=factor_name)

    # 临时保存一下数据，for 单元测试用
    # mean_quantile_ret_bydate.to_csv("test/data/mean_quantile_ret_bydate.csv")

    print("factor_returns 因子和股票收益率整合数据\n", factor_returns)
    print("mean_quantile_ret_bydate 分层的收益率的每期数据，这个最重要\n", mean_quantile_ret_bydate)  # !!!

    ic_data, (t_values, p_value, skew, kurtosis) = \
        create_information_tear_sheet(factor_data, group_neutral, by_group, set_context=False, factor_name=factor_name)

    print("ic_data:", ic_data)
    print("t_stat:", t_values)  # 这个是IC们的均值是不是0的检验T值
    # print("p_value:", p_value)
    # print("skew:", skew)
    # print("kurtosis:", kurtosis)

    __score, retuns_filterd_by_period_quantile = score(ic_data, t_values, mean_quantile_ret_bydate, periods)

    # 画出因子的多个期间的累计收益率的发散图
    plot_quantile_cumulative_returns(retuns_filterd_by_period_quantile, factor_name, periods)

    # create_turnover_tear_sheet(factor_data, set_context=False,factor_name=factor_name)
    return __score


def score(ic_data, t_values, mean_quantile_ret_bydate, periods):
    """
    基本思路是，不想看各种生成plot图，也可以评价出来一个因子好不好，通过给他打分，还知道有多好
    当然，图也留着，测试用，对照用。

    因子打分，参考：https://github.com/Jensenberg/multi-factor ； https://zhuanlan.zhihu.com/p/24616859
    - IC≠0的T检验的t值>2的比例要大于60%（因为是横截面检验，存在50只股票，所以要看着50只的）
    - 因子的偏度要 <0.05的比例大于60%，接近于正态（因为是横截面检验，存在50只股票，所以要看着50只的）
    - TODO 信息系数 ？？？
    - TODO 换手率
    IC是横截面上的50只股票对应的因子值和10天后的收益率的Rank相关性，
    之前理解成1只股票和它的N个10天后的收益率的相关性了，靠，基本概念都错了

    ========
    得分规则：
    ========
    - TODO: period单调性：就是看分组平均收益是否和按照分组递增或递减 +1分
    - TODO: 分组中的top组每日收益 - bottom组每日收益，的平均收益是否大于0，这个是看多空差异明显 +1分
    - TODO: 平均收益 +1
    - IC的T检验是不是>2，检验的其实是IC均值是不是≠0，t值>2，就是显著≠0，也就是收益和因子相关
    - 看分层quantile的发散比例（就是和quantile排序不一致）>90%, 发散程度给个3分，
    得到一个综合得分，且，可以把评分原因print出来，就算完成了，
    """

    scores = np.array([0] * len(periods))  # 几个换仓周期就有几个成绩

    """
    1.看IC≠0的T检验，是根据IC均值不为0
                 1D          5D          10D         20D
    t_stat: [ 0.15414273  0.44857945 -1.91258305 -5.01587993]
                0             0            0        1
    """
    t_values = np.array(t_values)
    t_flags = np.abs(t_values) > 2  # T绝对值大于2，说明
    t_values = t_flags + [0] * len(t_flags)  # 把True,False数组=>1,0值的数组
    scores += t_values

    """
    2.看IR是不是大于0.02
    """
    ir_data = ic_data.apply(lambda df: np.abs(df.mean() / df.std()))
    ir_flags = ir_data > 0.02
    ir_values = ir_flags + [0] * len(ir_flags)  # 把True,False数组=>1,0值的数组
    scores += ir_values

    """
    3. 看收益率
    3.1 看分组的收益率是不是可以完美分开
    3.2 top组和bottom组的收益率的均值的差值的平均值
    3.3 累计所有股票的收益率（所有的股票的累计收益率的平均值）
    ---------------------------------------------------------------
    mean_quantile_ret_bydate 分层的收益率的每期数据的样例，参考用
                                       1D        5D       10D
    factor_quantile date
    1               2020-01-02 -0.006622  0.032152  0.031694
                    2020-01-03  0.015522  0.051757  0.057915
                    2020-01-06  0.007057  0.012068  0.030915
                    2020-01-07  0.020194  0.013098  0.027879
                    2020-01-08 -0.004523  0.005561 -0.007309
    ...                              ...       ...       ...
    5               2020-11-11  0.005672  0.093278  0.105856
                    2020-11-12  0.050313  0.082732  0.080722
                    2020-11-13 -0.001784  0.052973  0.039285
    ---------------------------------------------------------------                    
    """
    monotony_percents, retuns_filterd_by_period_quantile = \
        calc_monotony(mean_quantile_ret_bydate, periods)

    monotony_percents = np.array([x.item() for x in monotony_percents])
    monotony_percents_flags = monotony_percents > 0.9
    monotony_percents_values = monotony_percents_flags + [0] * len(monotony_percents_flags)
    monotony_percents_values *= 3
    logger.debug("monotony_percents:\n%r", monotony_percents.tolist())

    scores += monotony_percents_values

    return scores, retuns_filterd_by_period_quantile

    """
    下午：TODO
    [x] 完成累计的10D、5D的计算，形成一个累计收益率检测，90%的才可以
    - 平均收益，1D的（不分组的），收益率的均值、正收益>负收益的比例
    - top - bottom？都应该是正的，
    - 如何考虑因子的方向？
    - 每个period中，不同分组的单调性？
    
    period单调性 +1
    top-bottom 大于多少， +1
    平均收益 +1
    发散>90%, 发散程度给个3分，
    得到一个综合得分，且，可以把评分原因print出来，就算完成了，
    这样就可以评价一个因子好不好，且给出解释原因，且，可以参考图，相互验证，赞了！
    debug/clv/
    """


def calc_monotony(mean_quantile_ret_bydate, periods):
    """
    计算单调性：
    看每一调仓日，分组的序号，是不是和收益值的序号一致，你知道的，如果是的单调，这俩的顺序应该是一直保持一致
    而我只需要算算所有的调仓日，不一致的情况超过一个阈值(目前默认是10%)，就可以判断是不是单调，
    另外，我还可以计算一下最后一天的最高一组和最低一组的收益差，这个也是重要指标，差越大越好，
    如果是负的这个因子是负向因子
    :return:
    """
    retuns_filterd_by_period_quantile = filterd_by_period_quantile(mean_quantile_ret_bydate, periods)

    monotony_percents = []

    def check_oneday_quantile_comply_rate(s: Series):
        """
        按照收益率排序，看看，是不是和分组quantile的顺序还一致：True|False
        如果和quantile的顺序，如1,2,3,4,5一致，返回True，否则是False
        这个主要用来计算这一天的数据，包含了截面上的所有的分组（分组的平均收益率）
        """
        assert type(s) == Series or type(s) == DataFrame
        if type(s) == DataFrame:
            assert len(s.columns) == 1
            s = s.iloc[:, 0]  # 如果是dataframe，一定只有1列，那么强制转成series，否则后面sort_values还需要指定列名，我们没法知道动态的列名
        indices_sort_by_value = s.sort_values().index.get_level_values('factor_quantile')
        indices_original = s.index.get_level_values('factor_quantile')
        return (indices_sort_by_value == indices_original).all()

    for i, returns_quantile in enumerate(retuns_filterd_by_period_quantile):
        days = periods[i]
        # 比比，收益率排序的索引，和，不排序的索引
        # 得到一个True，False索引差异数组，每个调仓日一个True/False
        df_order = returns_quantile.groupby(level='date').apply(check_oneday_quantile_comply_rate)

        # 统计一下True的，也就是一致的占比
        # import pdb;pdb.set_trace()
        monotony_percent = df_order.sum() / df_order.count()  # sum()可统计True的格式，诡异哈
        monotony_percents.append(monotony_percent)
        logger.debug("对每隔%d天的累计收益率中，有%.0f%%是和分组顺序一致的", days, monotony_percent)
    return monotony_percents, retuns_filterd_by_period_quantile


def filterd_by_period_quantile(mean_quantile_ret_bydate, periods):
    """
    这里有个细节，要算分组的时候，调仓周期period不是1天的时候，需要特殊处理。
    比如调仓周期为3天，
    2021-1-1    0.001
    2021-1-4    0.001
    2021-1-5    0.001
    2021-1-6    0.001
    2021-1-7    0.001
    现在我要算3天的累计收益，去画这个quantile分组图，
    我的横坐标就不是每天了，而是每隔3天，这个组的平均累计收益，
    这个值不太好画，我首先得确定起点，因为其他不同，画出来的日子也不同，
    举例，2021-1-1开始，那下个日子就是2021-1-6；但如果是2021-1-2开始，那下个日子就是2021-1-7。
    所以，我要算累计的话，就得确定一个开始日子，才可以算出最终的日子的累计收益率，
    可是仔细一想，开始选的日子不同，后面结束的日子也会错位，而不一定能到最后一天，对吧。纠结哈！
    
    1,2,3,4,5,6,7,8,9,10,11,12,13,14
    1,2,3,1,2,3,1,2,3,1, 2, 3, 1 ,2
      ^     ^     ^      ^        ^  --> 方案1，从最后一天开始往回算，每隔N天，用那天的因子得到的分组，做分组收益计算 
    ^     ^     ^     ^        ^     --> 方案2，始终从第一天开始，每隔N天，缺点是后面会空几天用不上
    但是，我是觉得，第二种方案更简单，但是有个问题是当周期很长，比如60天的时候，可能方案1更好，毕竟，更靠近当下的数据越接近现实
    不过，不用太纠结，我就选择一个简单的把，即方案1
              
    好在是alphalens，在每天，都给所有的股票分组了，他返回的那个mean_quantile_ret_bydate
    里每天都算了一遍当天的因子排名分组后的平均收益了，我可以直接拿来用：
                                       1D        5D       10D
        factor_quantile date
        1               2020-01-02 -0.006622  0.006349  0.003125 <--- 2020-1-2 我就挑这天的
                        2020-01-03  0.015522  0.010144  0.005646
                        2020-01-06  0.007057  0.002402  0.003049
                        2020-01-07  0.020194  0.002606  0.002754 <--- 2020-1-7 然后隔3天，就是这天了
                        2020-01-08 -0.004523  0.001110 -0.000733
        ...                              ...       ...       ...
        5               2020-11-11  0.005672  0.017996  0.010113 
                        2020-11-12  0.050313  0.016025  0.007793
                        2020-11-13 -0.001784  0.010377  0.003861
                        2020-11-16  0.013559  0.009161  0.004584
                        2020-11-17  0.019855  0.001982  0.001917
    
    这个是最开始的factor_returns的数据，放在这里方便理解，就是用他，来算的上述"mean_quantile_ret_bydate"的
           -------------------------------------------------------------------
                      |       | 1D  | 5D  | 10D  |factor|group|factor_quantile
           -------------------------------------------------------------------
               date   | asset |     |     |      |      |     |
           -------------------------------------------------------------------
                      | AAPL  | 0.09|-0.01|-0.079|  0.5 |  G1 |      3
                      --------------------------------------------------------
                      | BA    | 0.02| 0.06| 0.020| -1.1 |  G2 |      5
                      --------------------------------------------------------
           2014-01-01 | CMG   | 0.03| 0.09| 0.036|  1.7 |  G2 |      1
                      --------------------------------------------------------
                      | DAL   |-0.02|-0.06|-0.029| -0.1 |  G3 |      5
                      --------------------------------------------------------
                      | LULU  |-0.03| 0.05|-0.009|  2.7 |  G1 |      2
                      --------------------------------------------------------
    可以看出来，2014-1-1这天，按照因子排序，每个股票都有一个factor_quantile，就是这天的分组，注意，这个是每天都有这个信息。 
    """
    retuns_filterd_by_period_quantile = []
    for col, days in enumerate(periods):
        # 处理不同的时间间隔（days），即某一列（1D，10D，...)，他的顺序恰好和periods中的序号相同

        # 对于不同的时间间隔内，找到每一个分组，组内按照时间挑出间隔为days的行数据
        # 需要调整一下index，否则会出现重复index，参考
        #       https://stackoverflow.com/questions/38948336/why-groupby-apply-return-duplicate-level/51106729
        __returns = mean_quantile_ret_bydate.iloc[:, col].reset_index()  # 为了分组，先把index变成列，否则会出现重复index的问题
        __returns.groupby('factor_quantile').apply(lambda df: df.iloc[::days])  # 这个时候df是个series
        __returns.reset_index(drop=True)  # 上面的apply很诡异，会造成一个莫名的联合index，没用，drop掉
        __returns = __returns.set_index(["factor_quantile", "date"])

        logger.debug("每隔%d天挑出来的这%d累计收益率:\n%r", days, days, __returns)

        # 还要计算累计收益率
        __returns = (1 + __returns).cumprod() - 1

        logger.debug("每隔%d天，从开始的累计收益率:\n%r", days, __returns)

        retuns_filterd_by_period_quantile.append(__returns)
        logger.debug("按照%d天从分组收益率%d行中，过滤出%d行", days, len(mean_quantile_ret_bydate), len(__returns))
        logger.debug("过滤的结果：%r ~ %r", __returns.index[0], __returns.index[-1])
    return retuns_filterd_by_period_quantile


def synthesize(stock_pool, start_date, end_date):
    """测试因子合成"""
    stock_codes = get_stocks(stock_pool, start_date, end_date)
    factors = {}
    for factor_key in FACTORS.keys():
        factors[factor_key] = get_factors(factor_key, stock_codes, start_date, end_date)
    logger.debug("开始合成因子：%r", factors.keys())
    combined_factor = multifactor_synthesize.synthesize(factors, None)
    return combined_factor


def plot_quantile_cumulative_returns(quantile_cumulative_returns, factor_name, periods):
    plt.clf()
    f, axes = plt.subplots(len(quantile_cumulative_returns), 1, figsize=(18, 6))

    # 遍历每一个周期（1D，5D，10D）的数据
    for i, one_period_quantile_cumulative_returns in enumerate(quantile_cumulative_returns):
        ax = axes[i] # 1D,5D,10D对应的axis
        color = cm.rainbow(np.linspace(0, 1, 5))
        ymin, ymax = one_period_quantile_cumulative_returns.min(), one_period_quantile_cumulative_returns.max()
        ax.ylabel = 'Log Cumulative Returns'
        # ax.title = '''Cumulative Return by Quantile[{}D] for factor {}'''.format(periods[i],factor_name)
        ax.xlabel = ''
        ax.yscale = 'symlog'
        ax.yticks = np.linspace(ymin, ymax, 5)
        ax.ylim = (ymin, ymax)

        # 按照分组quantile，进行groupby，然后画图
        one_period_quantile_cumulative_returns = one_period_quantile_cumulative_returns.reset_index()
        factor_quantile_groups = one_period_quantile_cumulative_returns.groupby('factor_quantile')
        for quantile_index,group_data in factor_quantile_groups:
            group_data.columns = ["factor_quantile","date","returns"]
            print(group_data[['date','returns']])
            print("----------------->",quantile_index)

            group_data.plot(x='date',
                            y='returns',
                            ax=ax,
                            c=color[int(quantile_index)-1])

    tears.plot_image(factor_name=factor_name)


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
    periods = [1, 5, 10, 20, 40, 60]
    stock_pool = '000905.SH'  # 中证500
    stock_num = 50  # 用股票池中的几只，初期调试设置小10，后期可以调成全部

    # 调试用
    start = "20200101"
    end = "20200901"
    periods = [1, 5, 10]
    stock_pool = '000905.SH'  # 中证500
    stock_num = 10  # 用股票池中的几只，初期调试设置小10，后期可以调成全部

    # test_by_alphalens("clv", stock_pool, start, end, periods, stock_num)
    # test_by_alphalens("momentum", stock_pool, start, end, periods, stock_num)
    # test_by_alphalens("market_value", stock_pool, start, end, periods, stock_num)

    scores = []
    for factor_name, _ in FACTORS.items():
        __score = test_by_alphalens(factor_name, stock_pool, start, end, periods, stock_num)
        scores.append([factor_name, __score])

    for factor_name, __score in scores:
        logger.debug("换仓周期%r的 [%s]因子得分 分别为：%r", periods, factor_name, __score.tolist())

    # 测试多因子合成
    # combinefactor = synthesize(stock_pool, start, end)

    # 测试JAQS多因子合成
    # stock_codes = get_stocks(stock_pool, start, end)
    # combinefactor = synthesize_by_jaqs(stock_codes, start, end)
    # logger.debug("合成因子：")
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    #     print(combinefactor)  # .dropna(how="all").head())
