"""
- 回归法
    - t值绝对值均值：判断显著性
    - b. 因子收益率大于0的占比：判断该因子对股票收益率的正向影响是否明显
    - c. t值绝对值中大于2的占比：判断显著性是否稳定
    - d. 因子收益率零假设的t值：判断该因子的收益率序列是否显著不为零。
- IC法
    - 比较IC值序列的均值大小（因子显著性）、标准差（因子稳定性）、IR比率（因子有效性），以及累积曲线（随时间变化效果是否稳定）
    - 因子方向的指标一般有正相关显著比例、负相关显著比例、同向比例和状态切换比例: 市场风格是会轮动的，IC值可能会切换正负号，所以在选择因子时会计算相关系数的正负比例，并选择比例高的方向。 作为假如同向显著比例占上风，则意味着该段时间内因子的风格延续性较强，可以使用动态权重来调整因子的权重；若状态切换比例占上风，对于因子的赋权应该使用静态权重。
    - 使用移动平均线，对各因子在一定时间内的趋势进行横向比较，同时参考当时的重大市场行情变化
- 分层法
    - 年化收益率、夏普比率、信息比率、最大回撤
"""

import logging

import numpy as np
from pandas import Series, DataFrame

logger = logging.getLogger(__name__)


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
        logger.debug("对每隔%d天的累计收益率中，有%.2f%%是和分组顺序一致的", days, monotony_percent * 100)
    return monotony_percents, retuns_filterd_by_period_quantile


def filterd_by_period_quantile(mean_quantile_ret_bydate, periods):
    """
    按照periods中的调仓期，去挑选出累计调仓日的累计收益率。
    mean_quantile_ret_bydate内存放着调仓期的中每日的收益率，
    我们并不需要，我们只需要每隔调仓日的那天的收益率。
    比如调仓日是5天，那我们我们需要的是1号，5号，10号，...
    中间的2，3，4，6，7，8，9...都不需要

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
        __returns = __returns.groupby('factor_quantile').apply(lambda df: df.iloc[::days])  # 这个时候df是个series
        __returns.reset_index(drop=True)  # 上面的apply很诡异，会造成一个莫名的联合index，没用，drop掉
        __returns = __returns.set_index(["factor_quantile", "date"])

        logger.debug("每隔%d天挑出来的这%d累计收益率(只显示3行):\n%r", days, days, __returns.head(3))

        # 还要按照每组，计算这个组内的，累计收益率
        __returns = __returns.groupby('factor_quantile').apply(lambda df: (1 + df).cumprod() - 1)

        logger.debug("每隔%d天，从开始的累计收益率(只显示3行):\n%r", days, __returns.head(3))

        retuns_filterd_by_period_quantile.append(__returns)
        logger.debug("按照%d天从分组收益率%d行中，过滤出%d行", days, len(mean_quantile_ret_bydate), len(__returns))
        logger.debug("过滤的结果：%r ~ %r", __returns.index[0], __returns.index[-1])
    return retuns_filterd_by_period_quantile


def score_regression(df_result, t_values, factor_returns):
    """
    考察，回归法的效果

    - t值绝对值均值：判断显著性
    - b. 因子收益率大于0的占比：判断该因子对股票收益率的正向影响是否明显
    - c. t值绝对值中大于2的占比：判断显著性是否稳定 (t检验的是因子收益率不为0，即H_0）
    - d. 因子收益率零假设的t值：判断该因子的收益率序列是否显著不为零。
    """
    # 因子收益均值
    return_mean = factor_returns.apply(lambda s: s.mean(), axis=0)
    # 因子正收益的比例
    return_positive_rate = factor_returns.apply(lambda s: len(s[s > 0]) / len(s), axis=0)

    # 因子收益（回归出来的）不为0的t检验显著性(T的绝对值的均值)
    tvalue_mean = t_values.apply(lambda s: s.mean(), axis=0)
    tvalue_significant_rate = t_values.apply(lambda s: len(s[np.abs(s) > 2]) / len(s), axis=0)

    df_result = __result(df_result, "因子平均收益", "return_mean", return_mean)
    df_result = __result(df_result, "因子正收益比", "return_positive_rate", return_positive_rate)
    df_result = __result(df_result, "因子收益T显著", "tvalue_significant_rate", tvalue_significant_rate)
    df_result = __result(df_result, "因子收益T均值", "tvalue_mean", tvalue_mean)

    return df_result


def __result(df_result, title_cn, title_en, values):
    """
    :param df_result:
    :param title: 指标的名字
    :param values: 包含了多列（多调仓期）的metrics值
    :return:
    """
    return df_result.append(DataFrame([[title_cn, title_en] + values.tolist()], columns=df_result.columns))


def score_quantile(df_result, mean_quantile_ret_bydate, periods):
    """
    考察，分层收益率的效果

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
    logger.debug("monotony_percents:\n%r", monotony_percents)

    df_result = __result(df_result, "分层一致比例", "quantile_monotony_percents", monotony_percents)

    return df_result, retuns_filterd_by_period_quantile


def score_ic(df_result, ic_data, t_values, skew, kurtosis):
    """
    参数：
    :param ic_data: 相关性数值，shape[Days,N],
            days是多少天,比如从2020-1-1~2022-1-1，
            N就是调仓周期periods，就是1D，5D，20D
    :param t_values:
    :param mean_quantile_ret_bydate: 按照每天计算的平均的分组(quantile)的收益率
            比如 2020-1-1，第一组，平均收益率是0.0021
    :param periods: 就是调仓周期，如[1,5,20]

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

    """
    # 虽然前面有统计，我还是自己算一遍，
    # 不用绝对值，因为，在意方向
    IC值的分布，其实不用太在意是不是正态分布，这里考察偏度和峰度，我自己觉得没有太多意义，
    因为，如果好的话，所有的IC(每天一个IC值)都应该是均匀分布，好的话，都是1，都是正相关（极端地想）。
    """
    df_result = __result(df_result, "IC均值    ", "ic_mean", ic_data.apply(np.mean))
    df_result = __result(df_result, "IC标准差  ", "ic_std", ic_data.apply(np.std))
    df_result = __result(df_result, "IC分布偏度", "ic_skew", skew)
    df_result = __result(df_result, "IC分布峰度", "ic_kurtosis", kurtosis)

    """
    上面的t_value和p_value，是检验的啥？
    T检验，H_0假设是 ic_data的均值=0(popmean)， 参考：http://www.noobyard.com/article/p-dihbhkqa-nw.html
    IC均值为0，就意味着因子和收益不相关啊，
    所以，这个H_0的t值，我们期盼的是越大越好，越大说明p_value（概率）越小，原假设越不成立，
    即IC（均值）等于0就的可能性越小，这样，这个因子越有效。
    """
    logger.debug("IC均值为0的T值(越大越好>2):\n%r", t_values)

    """
    1.看IC≠0的T检验，是根据IC均值不为0
                 1D          5D          10D         20D
    t_stat: [ 0.15414273  0.44857945 -1.91258305 -5.01587993]
    """
    df_result = __result(df_result, "IC显著≠0T值", "ic_0_tvalue", t_values)

    """
    2.看IR是不是大于0.02，IR = IC的多周期均值 / IC的标准差，
    """
    ir_data = ic_data.apply(lambda df: np.abs(df.mean() / df.std()))
    df_result = __result(df_result, "IR值     ", "IR", ir_data)

    # ir_flags = ir_data > 0.02
    # ir_values = ir_flags + [0] * len(ir_flags)  # 把True,False数组=>1,0值的数组
    # df_result = __result(df_result, "IR值     ", ir_data)

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
    return df_result


def score(factor_return_0_t_vlues,
          factor_returns,
          ic_data,
          ic_mean_0_t_values,
          skew,
          kurtosis,
          mean_quantile_ret_bydate,
          periods):
    """
    :param factor_return_0_t_vlues: 因子收益率为0的T检验值
    :param factor_returns: 因子收益率
    :param ic_data: IC的结果
    :param ic_mean_0_t_values: IC结果为0的T检验值
    :param mean_quantile_ret_bydate: 每日的每组的调仓周期的平均收益率
    :param periods: 调仓周期
    :return:
    """
    df_result = DataFrame(columns=['name_cn', 'name_en'] + factor_returns.columns.tolist())

    df_result = score_regression(df_result, factor_return_0_t_vlues, factor_returns)

    df_result = score_ic(df_result, ic_data, ic_mean_0_t_values, skew, kurtosis)

    df_result, retuns_filterd_by_period_quantile = score_quantile(df_result, mean_quantile_ret_bydate, periods)

    logger.debug("因子各项指标评价：\n")
    print(df_result.to_markdown())

    return df_result, retuns_filterd_by_period_quantile
