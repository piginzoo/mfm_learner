import argparse
import logging

import matplotlib
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from alphalens import tears
from alphalens.tears import create_information_tear_sheet, create_returns_tear_sheet
from alphalens.utils import get_clean_factor_and_forward_returns, get_forward_returns_columns
from pandas import DataFrame

from datasource import datasource_factory, datasource_utils
from example import factor_utils
from example.analysis.score import score
from utils import utils, db_utils

logger = logging.getLogger(__name__)
datasource = datasource_factory.create()


def test_by_alphalens(factor_name, factors, df_stocks, index_prices, df_stock_basic, df_mv, periods):
    """
    用AlphaLens有个细节，就是你要防止未来函数，

    第二个输入变量是股票的价格数据，它是一个二维数据表(DataFrame)，行是时间，列是股票代码。
    第一是输入的价格数据必须是正确的， 必须是按照信号发出进行回测的，否则会产生前视偏差(lookahead bias)或者使用 到“未来函数”，
    可以加一个缓冲窗口递延交易来解决。例如，通常按照收盘价的回测其实就包含了这样的前视偏差，所以递延到第二天开盘价回测。
    """

    factors = factor_utils.preprocess(factors)

    # 中性化后的
    factors = factor_utils.neutralize(factors, df_stock_basic, df_mv)

    # column为股票代码，index为日期，值为收盘价
    df_stock_close = df_stocks.pivot_table(index='datetime', columns='code', values='close')

    # 过滤掉，factor因子中不包含的日期，为了将来对齐用，否则，报一个很诡异的datetime的set freq的异常
    df_stock_close = df_stock_close[df_stock_close.index.isin(factors.index.get_level_values('datetime'))]

    """
    这个是数据规整，
    factors - 必须是 index=[日期|股票], factor value
    prices - 行情数据，一般都是收盘价，index=[日期]，列是所有的股票
    groups - 行业归属数据，就是每天、每只股票隶属哪个行业：index=[日期], 列是：[股票，它归属的行业代码]
    """
    factor_data = get_clean_factor_and_forward_returns(factors, prices=df_stock_close, periods=periods)

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

    logger.debug("factor_returns 因子和股票收益率整合数据(只显示3行)\n%r", factor_returns.head(3))
    logger.debug("mean_quantile_ret_bydate 分层的收益率的每期数据，这个最重要(只显示3行)\n:%r", mean_quantile_ret_bydate.head(3))  # !!!

    ic_data, (ic_mean_0_t_values, p_value, skew, kurtosis) = \
        create_information_tear_sheet(factor_data,
                                      group_neutral,
                                      by_group,
                                      set_context=False,
                                      factor_name=factor_name)

    logger.debug("ic_data(只显示3行):\n%r", ic_data.head(3))
    logger.debug("ic_mean_0_t_values(只显示3行):\n%r", ic_mean_0_t_values[:3])  # 这个是IC们的均值是不是0的检验T值

    factor_return_0_t_vlues, factor_returns = factor_returns_regression(factor_data)

    df_result, retuns_filterd_by_period_quantile = \
        score(factor_return_0_t_vlues,
              factor_returns,
              ic_data,
              ic_mean_0_t_values,
              skew,
              kurtosis,
              mean_quantile_ret_bydate,
              periods)

    # 画出因子的多个期间的累计收益率的发散图
    plot_quantile_cumulative_returns(retuns_filterd_by_period_quantile,
                                     factor_name,
                                     periods,
                                     index_prices)

    # create_turnover_tear_sheet(factor_data, set_context=False,factor_name=factor_name)
    # return __score

    return df_result


def factor_returns_regression(factor_data):
    """
    入参，factor_data:
           ----------------------------------------------
                      |       | 1D  | 5D  | 10D  |factor|
           ----------------------------------------------
               date   | asset |     |     |      |      |
           ----------------------------------------------
                      | AAPL  | 0.09|-0.01|-0.079|  0.5 |
                      -----------------------------------
                      | BA    | 0.02| 0.06| 0.020| -1.1 |
                      -----------------------------------
           2014-01-01 | CMG   | 0.03| 0.09| 0.036|  1.7 |
                      -----------------------------------
        返回:
        t_values: shape[Days,Periods]
        factor_returns: shape[Days,Periods]
        Days，就是有多少天，如2020-1-1~2022-1-1
        Period，就是调仓周期，如[1,5,20]
    """

    def cross_section_regression(date_data):
        """
        截面回归，每一天，50只股票的收益，50只股票的因子，
        回归出：因子收益率、T值

        R_i = alpha + beta_i * f + e_i
        f就是因子收益
        """

        def regression(returns, factors):
            factors = sm.add_constant(factors)
            model = sm.OLS(returns, factors)  # 定义x，y
            results = model.fit()
            t_value = results.tvalues[1]
            factor_return = results.params[1]
            # import pdb;pdb.set_trace()
            return t_value, factor_return

        factors = date_data['factor']
        # get_forward_returns_columns会返回1D,2D,...的列名，是alphalens内嵌的函数
        # 针对每个日子，都进行处理
        tvalues = []
        factor_returns = []

        for column in get_forward_returns_columns(date_data.columns):
            returns = date_data[column]
            t_value, factor_return = regression(returns, factors)  # 这个是一天
            tvalues.append(t_value)
            factor_returns.append(factor_return)
        return tvalues, factor_returns

    # 因为要重新分组，所以先拷贝一个
    factor_data = factor_data.copy()
    # 按照日期分组
    groups = factor_data.groupby(level='date')

    df_factor_returns = []
    df_tavlues = []
    for name, df_cross_section in groups:
        # 按照日期分组计算
        tvalues, factor_returns = cross_section_regression(df_cross_section)
        df_factor_returns.append(factor_returns)
        df_tavlues.append(tvalues)
    column_names = get_forward_returns_columns(factor_data.columns)
    date_index = factor_data.index.unique(level='date')
    df_tavlues = DataFrame(df_tavlues, index=date_index, columns=column_names)
    df_factor_returns = DataFrame(df_factor_returns, index=date_index, columns=column_names)
    return df_tavlues, df_factor_returns


def plot_quantile_cumulative_returns(quantile_cumulative_returns, factor_name, periods, index_prices, quantile=5):
    """
    画图，画累计收益率的发散图
    :param quantile_cumulative_returns: 累计收益率
    :param factor_name:  因子名称
    :param periods: [1,5,10]这样的周期，和累计收益率个数对应
    :param index_prices: 对应的指数（基准）的价格
    :param quantile: 分组个数，默认为5
    :return:
    """
    plt.clf()
    fig, axes = plt.subplots(len(quantile_cumulative_returns), 1, figsize=(18, 18))
    # subplots很诡异，如果只有1个subplot，返回的ax是个单数，而不是list，所以统一成list
    if type(axes) != list:
        axes = [axes]
    fig.tight_layout()  # 调整整体空白
    plt.subplots_adjust(wspace=0, hspace=0.3)  # 调整子图间距

    # 遍历每一个周期（1D，5D，10D）的数据
    for i, one_period_quantile_cumulative_returns in enumerate(quantile_cumulative_returns):
        ax = axes[i]  # 1D,5D,10D对应的axis
        color = cm.rainbow(np.linspace(0, 1, quantile))
        ymin, ymax = one_period_quantile_cumulative_returns.min(), one_period_quantile_cumulative_returns.max()
        ax.ylabel = 'Log Cumulative Returns'
        ax.set_title('Cumulative Return by Quantile[{}D] for factor {}'.format(periods[i], factor_name))
        ax.set_xlabel('')
        # ax.yscale = 'symlog'
        ax.yticks = np.linspace(ymin, ymax, 5)
        ax.ylim = (ymin, ymax)

        # 计算指数对应天数的收益率;只取对应相隔天数的收益率
        index_prices['datetime'] = datasource_utils.to_datetime(index_prices['datetime'])
        index_prices = index_prices.sort_values('datetime')
        index_prices['returns'] = factor_utils.pct_chg(index_prices['close'], periods[i])
        index_returns = index_prices[['datetime', 'returns']]
        # import pdb;pdb.set_trace()
        index_returns = index_returns.apply(lambda df: df.iloc[::periods[i]])  # 只保留相隔天数，变少

        index_returns.plot(x='datetime',
                           y='returns',
                           ax=ax,
                           label='Index Return',
                           c='g')

        # 按照分组quantile，进行groupby，然后画图
        # 先把索引去掉，把索引变成列
        one_period_quantile_cumulative_returns = one_period_quantile_cumulative_returns.reset_index()
        one_period_quantile_cumulative_returns = one_period_quantile_cumulative_returns.sort_values('date')
        factor_quantile_groups = one_period_quantile_cumulative_returns.groupby('factor_quantile')
        # legends = []
        for quantile_index, group_data in factor_quantile_groups:
            group_data.columns = ["factor_quantile", "date", "returns"]
            group_data.plot(x='date',
                            y='returns',
                            ax=ax,
                            label=str(int(quantile_index)),
                            c=color[int(quantile_index) - 1])
        ax.legend(loc="upper right")

    tears.plot_image(factor_name=factor_name)


def main(factor_names, start_date, end_date, index_code, periods, num):
    """
    :param index_code: 股票池/指数代码
    :param periods: 调仓周期，如 20,30
    :param num: 股票池中使用多少只作为测试子集，仅用于测试
    """

    pd.set_option('display.max_rows', 1000)
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
    matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

    stock_codes = datasource.index_weight(index_code, start_date, end_date)

    stock_codes = stock_codes[:num]

    # 加载股票的行情数据
    df_stocks = datasource.daily(stock_codes, start_date=start_date, end_date=end_date)
    df_stocks = datasource_utils.reset_index(df_stocks)

    # 市值数据，用于市值中性化
    df_mv = datasource.daily_basic(stock_codes, start_date, end_date)
    df_mv = datasource_utils.reset_index(df_mv)
    df_mv = df_mv['total_mv']

    # 股票的基本信息，主要为为了获得行业信息
    df_stock_basic = datasource.stock_basic(stock_codes)

    # 获得指数（股票池）的每日价格信息
    index_prices = datasource.index_daily(index_code, start_date=start_date, end_date=end_date)
    assert len(index_prices) > 0, index_prices

    for factor_name in factor_names:
        df_factor = factor_utils.get_factor(factor_name, stock_codes, start_date, end_date)
        if df_factor is None or len(df_factor) == 0:
            logger.warning("无法加载因子[%s]，请运行因子创建程序去创建因子：factor_create.py", factor_name)
            continue
        df_result = test_by_alphalens(factor_name, df_factor, df_stocks, index_prices, df_stock_basic, df_mv, periods)
        save_analysis_result(factor_name, df_result)
        logger.debug("换仓周期%r的 [%s]因子得分", periods, factor_name)


def save_analysis_result(factor_name, df_result):
    """
    将因子跑的结果，保存到数据库中
    :param factor_name:
    :param df_result:
    :return:
    """

    engine = utils.connect_db()

    # 先删除旧的因子分析结果
    if db_utils.is_table_exist(engine, "factor_analysis"):
        db_utils.run_sql(engine, f"delete from factor_analysis where factor='{factor_name}'")

    # 按照调仓周期不同，分别存成不同的记录，本来 1D,5D,10D是在列上
    # 要把他们分别保存，这样做是为了将来可以做横向对比
    all_columns = df_result.columns
    period_columns = get_forward_returns_columns(all_columns)
    without_period_columns = [c for c in all_columns if c not in period_columns]
    for period_column in period_columns:
        df_one_period = df_result[without_period_columns + [period_column]]
        df_one_period.columns = ['name_cn', 'name_en', 'metrics']
        df_one_period['factor'] = factor_name
        df_one_period['period'] = period_column
        df_one_period.to_sql('factor_analysis', engine, index=False, if_exists="append")


"""
# 测试用
python -m example.factor_analyzer \
    --factor roe_yoy \
    --start 20180101 \
    --end 20191230 \
    --num 50 \
    --period 20 \
    --index 000905.SH
"""
if __name__ == '__main__':
    utils.init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--factor', type=str, help="因子名|all是所有")
    parser.add_argument('-s', '--start', type=str, help="开始日期")
    parser.add_argument('-e', '--end', type=str, help="结束日期")
    parser.add_argument('-i', '--index', type=str, help="股票池code")
    parser.add_argument('-p', '--period', type=str, help="调仓周期，多个的话，用逗号分隔")
    parser.add_argument('-n', '--num', type=int, help="股票数量")
    args = parser.parse_args()

    if "," in args.period:
        periods = [int(p) for p in args.period.split(",")]
    else:
        periods = [int(args.period)]

    if "," in args.factor:
        factor_names = [f for f in args.factor.split(",")]
    elif args.factor == "all":
        factor_names = factor_utils.get_factor_names()
    else:
        factor_names = [args.factor]

    main(factor_names,
         args.start,
         args.end,
         args.index,
         periods,
         args.num)
