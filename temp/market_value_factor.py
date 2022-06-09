"""
参考：
https://zhuanlan.zhihu.com/p/161706770
"""
import datetime
import logging
import math
import time

from mfm_learner import utils

utils.init_logger()

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
import tushare
from matplotlib.font_manager import FontProperties
from scipy import stats as st

import data_provider

logger = logging.getLogger(__name__)

start_time = time.time()

__data_provider = data_provider.get("tushare")
__IS_PLOT = True


def LNCAP(universe, start, end, file_name="data/LNCAP.csv"):
    """
    市值因子
    :param universe:
    :param start:
    :param end:
    :param file_name:
    :return:
    """
    factor = pd.DataFrame()
    pro = tushare.pro_api()
    # 每支股票
    for stk in universe:
        # 得到日交易数据,计算因子
        data = pro.daily_basic(ts_code=stk, start_date=start, end_date=end)
        # logger.debug("获得%s的%s~%s的%d条交易数据",stk,start,end,len(data))
        factor_data = data.sort_values(['trade_date'])
        factor_data['LNCAP'] = np.log(factor_data['total_mv'])
        tmp_factor_data = factor_data[['trade_date', 'LNCAP']]
        tmp_factor_data.columns = ['trade_date', stk]
        if factor.empty:
            factor = tmp_factor_data
        else:
            factor = factor.merge(tmp_factor_data, on='trade_date', how='outer')
    factor = factor.set_index('trade_date')
    factor.to_csv(file_name)
    logger.debug("计算完市值因子(LNCAP)，从%s~%s，%d 条因子值", start, end, len(factor))

    # 画直方图
    if __IS_PLOT:
        plt.clf()
        fig = plt.figure(figsize=(50, 30))
        row = math.ceil(len(factor.columns) / 3)
        for i, column_name in enumerate(factor.columns):
            ax1 = fig.add_subplot(row, 3, i + 1)
            ax1.set_title(column_name)
            ax1.hist(factor.iloc[:, i], bins=100)
        plt.savefig("debug/原始因子值直方图.jpg")

    return factor


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
    factors = factors.apply(__fillna_series)  # 填充NAN
    factors = factors.apply(__winsorize_series)  # 去极值
    factors = factors.apply(__standardize_series)  # 标准化
    logger.debug("规范化预处理完市值因子(LNCAP)，%d行", len(factors))
    if __IS_PLOT:
        plt.clf()
        fig = plt.figure(figsize=(50, 30))
        row = math.ceil(len(factors.columns) / 3)
        for i, column_name in enumerate(factors.columns):
            ax1 = fig.add_subplot(row, 3, i + 1)
            ax1.set_title(column_name)
            ax1.hist(factors.iloc[:, i], bins=100)
        plt.savefig("debug/预处理后的因子值直方图.jpg")

    return factors


# 获取股票历史每日市值
def getForwardReturns(universe, start, end, window, file_name):
    """
    每天都计算一下从当日，到当日+5日后的股票的收益率，所以最后5天没有值
    """
    # 计算个股历史区间前瞻回报率，未来windows天的回报率
    start_time = time.time()
    ret_data = pd.DataFrame()
    for stock in universe:
        data = __data_provider.basic(code=stock, start=start, end=end)
        tmp_ret_data = data.sort_values(['trade_date'])

        # 计算历史窗口的前瞻收益率
        tmp_ret_data['forwardReturns'] = tmp_ret_data['close'].shift(-window) / tmp_ret_data['close'] - 1.0
        tmp_ret_data = tmp_ret_data[['trade_date', 'forwardReturns']]
        tmp_ret_data.columns = ['trade_date', stock]

        if ret_data.empty:
            ret_data = tmp_ret_data
        else:
            ret_data = ret_data.merge(tmp_ret_data)

    ret_data.to_csv(file_name)
    ret_data = ret_data.set_index(['trade_date'])
    logger.debug("计算%d只股票的从%s到%s，%d日回报率,保存到：%s, 耗时%.2f秒", len(universe), start, end, window, file_name,
                 time.time() - start_time)
    return ret_data


def ic_test(factors, stock_returns):
    # """
    # IC法
    # 1.确定持仓周期，我们这里是5天
    # 2.计算每天股票对应的未来调仓周期的收益
    # 3.计算未来调仓周期收益率和因子收益率之间的相关系数
    # """
    # 计算每天的clv收益率和之后5天的股票收益率的秩的相关系数
    # 把第二列stkID中的股票值，变成列名，类似于数据透视表，把行数据转成了列，这样做是为了和5日收益率对齐
    # factors = factors[~factors.index.duplicated()]  # 有重复数据居然，剔除掉
    # import pdb;pdb.set_trace()
    # factors = factors.unstack('stkID')['factor']  # 可以把索引变成列，很神奇，但是，列名多了一个'factor'
    # factors = factors.rename_axis(columns=None)  # 行变列，参考：https://www.cnblogs.com/traditional/p/11967360.html

    # 计算相关系数,每天计算一个
    index = 1
    ic_data = pd.DataFrame(index=factors.index, columns=['IC', 'pValue'])
    for date in ic_data.index:
        """
        每天，找出所有的50只股票的对应的中性化后的因子暴露，是50个数，
        然后，和这一天，这50只股票对应的5日后的股票收益率，也是50个数，
        50个因子暴露：50个5天收益率，做corr相关性（spearmanr）计算，
        得到相关性系数，1个值和对应的1个p_value
        """
        day_factors = factors.loc[date]  # 得到日期的clv因子暴露，20只股票的
        if date not in stock_returns.index:
            logger.warning("5日回报率数据中没有日期 %s 的信息", date)
            continue
        return_5_days = stock_returns.loc[date]  # 得到日期的5天后的收益率

        corr = pd.DataFrame(day_factors)
        ret = pd.DataFrame(return_5_days)

        corr.columns = ['corr']
        ret.columns = ['ret']
        corr['ret'] = ret['ret']

        corr = corr[~np.isnan(corr['corr'])][~np.isnan(corr['ret'])]
        if len(corr) < 5:
            logger.warning("%s 的相关性计算数据，%d个不足5个，不予计算", date, len(corr))
            continue
        ic, p_value = st.spearmanr(corr['corr'], corr['ret'])  # 计算秩相关系数 Rank_IC
        ic_data['IC'][date] = ic
        ic_data['pValue'][date] = p_value
        index += 1

    logger.debug("计算出 %d 个相关系数（每日1个）", len(ic_data))

    # 给每天的factor,5日收益率,秩相关系数做图
    ic_data = ic_data.dropna()
    logger.debug("Drop NAN后，剩余 %d 个相关系数", len(ic_data))

    logger.debug("IC 均值：%.4f" % ic_data['IC'].mean())
    logger.debug("IC 中位数：%.4f" % ic_data['IC'].median())
    logger.debug("IC 值中有 %d个>0, %d个<0" % (len(ic_data[ic_data.IC > 0]), len(ic_data[ic_data.IC < 0])))

    if __IS_PLOT:
        plt.clf()
        font = FontProperties()
        fig = plt.figure(figsize=(16, 6))
        ax1 = fig.add_subplot(111)
        lns1 = ax1.plot(np.array(ic_data.IC), label='IC')
        lns = lns1
        labs = [l.get_label() for l in lns]
        ax1.legend(lns, labs, bbox_to_anchor=[0.5, 0.1], loc='best', mode='', borderaxespad=0., fontsize=12)
        ax1.set_xlabel("date", fontproperties=font, fontsize=16)
        ax1.set_ylabel("corr", fontproperties=font, fontsize=16)
        ax1.set_title("factor and five days interest rank corr", fontproperties=font, fontsize=16)
        ax1.grid()
        plt.savefig("debug/因子暴露和N日收益率的相关性.jpg")

    # 计算IC方法二
    stock_returns = stock_returns.stack()
    factors = factors.stack()
    combineMatrix = pd.concat([factors, stock_returns], axis=1, join='inner')
    combineMatrix.columns = ['factor', 'stock_return']

    DayIC = combineMatrix.groupby(level='trade_date').corr(method='spearman')
    DayIC = DayIC.reset_index().drop(['level_1'], axis=1)
    DayIC = pd.DataFrame(DayIC.loc[DayIC.factor != 1, 'factor'])
    DayIC.columns = ['IC']

    logger.debug("-----------------------")
    logger.debug("另一种方法算出的IC相关系数")
    logger.debug("IC 的均值：%.4f", DayIC.mean())
    logger.debug("IC 中位值：%.4f", DayIC.median())
    logger.debug("IC 值中有 %d 个 > 0, %d 个 < 0", len(DayIC[DayIC.IC > 0]), len(DayIC[DayIC.IC < 0]))


def calc_factor_returns(factors, stock_returns):
    """
    回归法算因子收益率
    1.首先将因子和未来收益率在界面上对齐（日期、代码）
    2.将未来的收益率作为因变量，因子作为自变量，回归计算出来的系数作为因子收益率
    3.计算因子收益率的t值等相关统计量
    """
    stock_returns = stock_returns.stack()
    factors = factors.stack()
    combineMatrix = pd.concat([factors, stock_returns], axis=1, join='inner')
    combineMatrix.columns = ['factor', 'stock_return']
    combineMatrix1 = combineMatrix.reset_index()  # 去掉联合索引['trade_date', 'stockID']，使之变成列
    combineMatrix1.columns = ['trade_date', 'stockID', 'factor', 'stock_return']

    # 按天回归，回归系数作为因子收益率
    unidate = factors.reset_index().trade_date.drop_duplicates()
    unidate = list(unidate)
    factor_returns = pd.DataFrame(columns=['factor_return', 't_value'], index=unidate)

    # 按照日期的横截面，做回归，Y是5日股票收率，X是因子暴露值，系数β为因子收益率，t_value为系数为0的置信度
    for d in unidate:
        tempdata = combineMatrix1.loc[combineMatrix1['trade_date'] == d, :]
        tempdata = tempdata.dropna()
        if len(tempdata) > 0:
            model = sm.OLS(np.array(tempdata.stock_return),
                           np.array(tempdata.factor))
            results = model.fit()
            factor_returns.loc[d, 'return_value'] = results.params[0]
            factor_returns.loc[d, 't_value'] = results.tvalues[0]

    """
    回归法因子检测
    1.计算t值绝对值的均值，看t值是不是显著不为0，有效性是>2
    2.t值绝对值大于2的比例-稳定性(比例大于40%)
    3.计算因子收益率的时间序列上的t值,是不是显著不为0 -- 风险因子？alpha因子？
    """
    # 1.计算t值绝对值的均值，看t值是不是显著不为0，--- 有效性
    logger.debug("T值绝对值均值: %.4f", factor_returns.t_value.abs().mean())
    # 2.t值绝对值序列大于2的比例 --- 稳定性
    logger.debug("显著比例(abs(T)>2的比例：%.2f%%",
                 len(factor_returns[factor_returns.t_value.abs() > 2]) / len(factor_returns) * 100)
    # 3.计算因子收益率的时间序列上的t值,是不是显著不为0 -- 风险因子？alpha因子？
    logger.debug("因子收益率均值: %.4f" % factor_returns.return_value.mean())
    logger.debug("因子收益率方差：%.4f" % factor_returns.return_value.std())
    logger.debug("因子收益率夏普指数%.4f" % (
            factor_returns.return_value.mean().item() / (factor_returns.return_value.std().item() + 0.0000001)))

    if __IS_PLOT:
        # 画图
        plt.clf()
        font = FontProperties()
        fig = plt.figure(figsize=(16, 6))
        ax1 = fig.add_subplot(111)
        lns1 = ax1.plot(np.array(factor_returns.return_value), label='Factor Return')
        lns = lns1
        labs = [l.get_label() for l in lns]
        ax1.legend(lns, labs,
                   bbox_to_anchor=[0.5, 0.1],
                   loc='best',
                   ncol=3,
                   mode='',
                   borderaxespad=0.,
                   fontsize=12)
        ax1.set_xlabel("Date", fontproperties=font, fontsize=16)
        ax1.set_ylabel("Return", fontproperties=font, fontsize=16)
        ax1.set_title("Factor Return", fontproperties=font, fontsize=16)
        ax1.grid()
        plt.savefig("debug/因子收益率.jpg")


def layerize_analyze(factors, stock_returns):
    """
    分层法分析
    """
    # 分层法检测
    n_quantile = 5
    # 统计十分位数
    cols = [i + 1 for i in range(n_quantile)]
    excess_returns_means = pd.DataFrame(index=factors.index, columns=cols)
    # 计算因子分组的超额收益平均值
    # excess_returns_means，是每天一个数，各个5分位的收益率均值（减去了总的平均收益率）
    # 1是因子值最小，5是因子值最大
    for date in excess_returns_means.index:
        qt_mean_results = []

        factor_factor = factors.loc[date].dropna()  # 删除clv中的nan

        if date not in stock_returns.index:
            logger.warning("5日回报率数据中没有日期 %s 的信息", date)
            continue
        stock_5days_return = stock_returns.loc[date].dropna()  # 删除股票5日回报率的nan
        stock_5days_return_mean = stock_5days_return.mean()  # 5日回报率的均值

        pct_quantiles = 1.0 / n_quantile  # n_quantile = 5
        for i in range(n_quantile):
            clv_factor_down = factor_factor.quantile(pct_quantiles * i)  # quantile - 分位数
            clv_factor_up = factor_factor.quantile(pct_quantiles * (i + 1))
            i_quantile_index = factor_factor[
                (factor_factor <= clv_factor_up) & (factor_factor > clv_factor_down)].index  # 在clv中找到对应分位数的股票代码
            if not i_quantile_index.isin(stock_5days_return.index).all():
                # logger.warning("Key[%s]不在当前5日汇报率中：%r",i_quantile_index,stock_5days_return)
                continue
            stock_return_mean = stock_5days_return.loc[
                                    i_quantile_index].mean() - stock_5days_return_mean  # 计算这些股票的5天收益率的平均值 - 总体均值
            # logger.debug("第%d组:因子值在 %.2f ~ %.2f 之间的股票%d只", i+1, clv_factor_down, clv_factor_up, len(i_quantile_index))

            qt_mean_results.append(stock_return_mean)

        if len(qt_mean_results) == n_quantile:
            excess_returns_means.loc[date] = qt_mean_results

    excess_returns_means.dropna(inplace=True)
    # excess_returns_means.info()
    logger.debug("一共耗时 ： %.2f 秒", (time.time() - start_time))
    excess_returns_means.to_csv("data/excess_returns_means.csv")

    # 画图
    if __IS_PLOT:
        plt.clf()
        font = FontProperties()
        fig = plt.figure(figsize=(16, 6))
        ax1 = fig.add_subplot(111)
        excess_returns_means_dist = excess_returns_means.mean()
        excess_dist_plus = excess_returns_means_dist[excess_returns_means_dist > 0]
        excess_dist_minus = excess_returns_means_dist[excess_returns_means_dist < 0]
        lns2 = ax1.bar(excess_dist_plus.index, excess_dist_plus.values, align='center', color='g', width=0.1)
        lns3 = ax1.bar(excess_dist_minus.index, excess_dist_minus.values, align='center', color='r', width=0.1)
        ax1.set_xlim(left=0.5, right=len(excess_returns_means_dist) + 0.5)
        ax1.set_ylim(-0.008, 0.008)
        ax1.set_ylabel("return", fontproperties=font, fontsize=16)
        ax1.set_xlabel("5 divisions", fontproperties=font, fontsize=16)
        ax1.set_xticks(excess_returns_means_dist.index)
        ax1.set_xticklabels([int(x) for x in ax1.get_xticks()], fontproperties=font, fontsize=14)
        ax1.set_yticklabels([str(x * 100) + "0%" for x in ax1.get_yticks()], fontproperties=font, fontsize=14)
        ax1.set_title("factor factor return interest", fontproperties=font, fontsize=16)
        ax1.grid()
        plt.savefig("debug/因子暴露{}分位所有股票所有日期的平均收益率.jpg".format(n_quantile))

        # 画不同分位的累计收益率
        plt.clf()
        excess_returns_means_cum = excess_returns_means.iloc[:, 1:n_quantile].apply(
            lambda x: (1 + x).cumprod().values - 1)
        font = FontProperties()
        fig = plt.figure(figsize=(16, 6))
        ax1 = fig.add_subplot(111)
        ax1.set_ylabel("return", fontproperties=font, fontsize=16)
        ax1.set_yticklabels([str(x * 100) + "0%" for x in ax1.get_yticks()], fontproperties=font, fontsize=14)
        ax1.set_title("{} quantiles accumulated return".format(n_quantile), fontproperties=font, fontsize=16)
        ax1.grid()
        plt.plot(excess_returns_means_cum.iloc[:, :])
        plt.legend(excess_returns_means_cum.columns.to_list())
        plt.savefig("debug/按因子{}分位的分组累计收益图.jpg".format(n_quantile))


def get_universe(name, start, num):
    # 开始计算factor
    end = datetime.datetime.now().strftime("%Y%m%d")
    universe = __data_provider.index_stocks(name, start=start, end=end)  # 股票池
    logger.debug("获得【%s】成分股%d只", name, len(universe))
    np.random.shuffle(universe)  # 计算速度缓慢，仅以部分股票池作为sample
    universe = universe[0:num]
    logger.debug("只保留%d只用于简化计算", num)
    return universe


def main(start, end, window_return, stock_pool, stock_num):
    utils.init_logger()
    matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
    matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题
    conf = utils.load_config()
    tushare.set_token(conf['tushare']['token'])

    universe = get_universe(stock_pool, start, stock_num)
    assert len(universe) > 0, str(len(universe))

    # 开始日~结束日，50只股票，计算这个因子，这个因子是，每天对应1个股票1个数，比如 2019-1-2，000001.XSHE股票，对应一个因子值，
    # 这个值，不是收益率。
    factors = LNCAP(universe=universe, start=start, end=end)
    factors = proprocess(factors)

    # 获得每天往后看5日的预期收益率
    forward_5d_stock_returns = getForwardReturns(
        universe=universe,
        start=start,
        end=end,
        window=window_return,
        file_name="data/ForwardReturns_W5_Rtn.csv"
    )

    ic_test(factors, forward_5d_stock_returns)

    calc_factor_returns(factors, forward_5d_stock_returns)

    layerize_analyze(factors, forward_5d_stock_returns)


# python market_value_factor.py
if __name__ == '__main__':
    # 实际运行
    # start = "20150101"
    # end = "20211201"
    # day_window = 10
    # stock_pool = '000300.SH'
    # stock_num = 100

    # 测试用
    start = "20210101"
    end = "20211201"
    day_window = 5
    stock_pool = '000300.SH'
    stock_num = 50  # 用股票池中的几只，初期调试设置小10，后期可以调成全部

    main(start, end, day_window, stock_pool, stock_num)
