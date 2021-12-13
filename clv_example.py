# coding: utf-8

# 第一个因子:
# clv: close location value,
# ( (close-day_low) - (day_high - close) ) / (day_high - day_low)
# 这玩意，是一个股票的，每天都有，是一个数，
# 我们要从一堆的股票N只中，得到N个这个值，可以形成一个截面，
# 用这个截面，我们可以拟合出β和α，
# 然后经过T个周期（交易日），就可以有个T个β和α，
# 因子长这个样子：
# trade_date
#  	      	000001.XSHE  000002.XSHE
# 2019-01-02  -0.768924    0.094851     。。。。。
# 。。。。。。
# 类比gpd、股指（市场收益率），这个是因子不是一个啊？而是多个股票N个啊？咋办？
#
# 参考：https://www.bilibili.com/read/cv13893224?spm_id_from=333.999.0.0
import logging
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from matplotlib.font_manager import FontProperties
from scipy import stats as st
import matplotlib

matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
matplotlib.rcParams['axes.unicode_minus'] = False  # 正常显示负号
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

import utils

utils.init_logger()
import data_provider

data_provider = data_provider.get('tushare')

logger = logging.getLogger(__name__)

start_time = time.time()

def CLV(universe, start, end, file_name):
    factor = pd.DataFrame()
    # 每支股票
    for stk in universe:
        # 得到日交易数据
        data = data_provider.bar(code=stk, start=start, end=end)
        # data.info()
        tmp_factor_data = data.sort_values(['trade_date'])
        # 计算CLV因子
        tmp_factor_data['CLV'] = ((tmp_factor_data['close'] - tmp_factor_data['low']) - (
                tmp_factor_data['high'] - tmp_factor_data['close'])) / (
                                             tmp_factor_data['high'] - tmp_factor_data['low'])
        # 处理出现一字涨跌停
        tmp_factor_data.loc[(tmp_factor_data['high'] == tmp_factor_data['low']) & (
                tmp_factor_data['open'] > tmp_factor_data['pre_close']), 'CLV'] = 1
        tmp_factor_data.loc[(tmp_factor_data['high'] == tmp_factor_data['low']) & (
                tmp_factor_data['open'] < tmp_factor_data['pre_close']), 'CLV'] = -1

        tmp_factor_data = tmp_factor_data[['trade_date', 'CLV']]
        tmp_factor_data.columns = ['trade_date', stk]

        if factor.empty:
            factor = tmp_factor_data
        else:
            factor = factor.merge(tmp_factor_data, on='trade_date', how='outer')

    factor = factor.set_index('trade_date')
    factor.to_csv(file_name)
    return factor


# 开始计算CLV
# start_date = '20190101'  # 开始日期
start_date = '20210601'  # 开始日期
end_date = '20211201'  # 结束日期
universe = data_provider.index_stocks('000300.SH', date=end_date)  # 股票池
logger.debug("获得【%s】成分股%d只", '000300.SH', len(universe))
universe = universe[0:10]  # 计算速度缓慢，仅以部分股票池作为sample
logger.debug("只保留50只用于简化计算")

# 开始日~结束日，50只股票，计算这个因子，这个因子是，每天对应1个股票1个数，比如 2019-1-2，000001.XSHE股票，对应一个因子值，
# 这个值，不是收益率。
CLV(universe=universe, start=start_date, end=end_date, file_name='data/CLV.csv')
CLVfactor = pd.read_csv('data/CLV.csv')
CLVfactor[["trade_date"]] = CLVfactor[["trade_date"]].astype(str) # 转成str，csv再加载后自动认成了int
CLVfactor = CLVfactor.set_index(['trade_date'])
CLVfactorstack = CLVfactor.stack()  # 按照trade_date，变成一个树状结构
CLVfactorstack.hist(figsize=(12, 6), bins=50)
logger.debug("计算完CLV因子")


# 去极值和标准化方法
def winsorize_series(se):
    q = se.quantile([0.025, 0.975])
    if isinstance(q, pd.Series) and len(q) == 2:
        se[se < q.iloc[0]] = q.iloc[0]
        se[se > q.iloc[1]] = q.iloc[1]
    return se


def standardize_series(se):
    se_std = se.std()
    se_mean = se.mean()
    return (se - se_mean) / se_std


factor_init = CLVfactorstack.groupby(level='trade_date').apply(winsorize_series)  # 去极值
factor_init = factor_init.groupby(level='trade_date').apply(standardize_series)  # 标准化
factor_init.hist(figsize=(12, 6), bins=50)


# 3.中性化（可选）
# 3.1中性化的优点：能够将新因子的已知部分剥离掉，剩下的部分为真正新的因子，这样我们能够真正看明白新因子是否真正是新的，还是仅仅原有因子的线性组合加噪音。
# 3.2中性化的缺点：计算复杂，每一个新因子都要进行一次中性化。作为基准的老因子之间，比如行业和市值因子之间也有相关性。
# 3.3中性化计算步骤：
# -确定将目标因子进行中性化的因子（市值因子，行业因子等等）
# -将目标因子做为因变量，将市值因子，行业因子做为自变量
# -运行回归，将回归的残差项做为中性化后的因子
# 3.4实操代码
# 3.4.1计算市值因子
def getMarketValueAll(universe, start, end, file_name):
    # 获取股票历史每日市值
    ret_data = pd.DataFrame()
    start_time = time.time()
    for stk in universe:

        logger.debug("获取股票【%s】的信息", stk)
        data = data_provider.basic(code=stk, start=start, end=end)  # 拿取数据
        tmp_ret_data = data.sort_values(['trade_date'])

        # 市值部分
        tmp_ret_data = tmp_ret_data[['trade_date', 'circ_mv']]
        tmp_ret_data.columns = ['trade_date', stk]
        if ret_data.empty:
            ret_data = tmp_ret_data
        else:
            ret_data = ret_data.merge(tmp_ret_data, on='trade_date', how='outer')

    ret_data.to_csv(file_name)

    logger.debug("获得从 %s~%s %d只股票的市值信息%d条，耗时：%.2f", start, end, len(universe), len(ret_data), time.time() - start_time)

    return ret_data


# 获取股票历史每日市值

# comnbinedfactor因子数据，是每天，多只股票
# 
#     trade_date	stkID	     CLV	        MVfactor_std
# 0	2019-01-02	000001.XSHE	-7.689243e-01	4.530452
# 1	2019-01-02	000002.XSHE	9.485095e-02	4.530452
# 2	2019-01-02	000004.XSHE	-5.652174e-01	-0.428141
# 3	2019-01-02	000005.XSHE	-5.000000e-01	-0.376903
# 4	2019-01-02	000006.XSHE	-3.382353e-01	-0.226704
# 5	2019-01-02	000007.XSHE	-7.142857e-02	-0.388427
# 6	2019-01-02	000008.XSHE	4.000000e-01	-0.120089
# 7	2019-01-02	000009.XSHE	-4.677419e-01	-0.147528
# 8	2019-01-02	000010.XSHE	-7.401487e-15	-0.417451
# 
# Y= βX + e
# CLV = β * MV + e
# 我们不关心β，我们只关心e，e就是新的因子。
# 注意，这里是每只股票的这两个值做回归，比如有50只股票，那就是50个值对做回归，
# 就得到了每天，这个因子（被MV市值中性化后的CLV）的值。
# 因子是每天一个值，而且，是每个股票一个值，这个有点和我想想的不一样了，不是因子是共通的么？
# 我理解应该是每天1个值，类比于市场因子（比如股指的收益率）


# ----------- 计算股票流通市值 ----------------
MVfactor = getMarketValueAll(universe=universe, start=start_date, end=end_date, file_name='data/marketvalue.csv')

# ----------- 去极值和标准化 ----------------
MVfactor = MVfactor.set_index(['trade_date'])
MVfactorstack = MVfactor.stack()
MVfactor_init = MVfactorstack.groupby(level='trade_date').apply(winsorize_series)  # 去极值
MVfactor_std = MVfactor_init.groupby(level='trade_date').apply(standardize_series)  # 标准化
MVfactor_std.hist(figsize=(12, 6), bins=50)
logger.debug("去除极值和标准化 %d 条", len(MVfactor_std))

# 3.4.5进行因子中性化
# 将个股收益率和因子对齐
comnbinedfactor = pd.concat([CLVfactorstack, MVfactor_std], axis=1, join='inner')
comnbinedfactor = comnbinedfactor.reset_index()  # reset_index()返回原index，这里就是trade_date
comnbinedfactor.columns = ['trade_date', 'stkID', 'CLV', 'MVfactor_std']
assert len(comnbinedfactor) != 0, str(comnbinedfactor)

# 按天进行回归，回归残差作为新因子
unidate = comnbinedfactor.trade_date.drop_duplicates()
unidate_list = list(unidate)

data = np.array([])
for d in unidate_list:
    # 按照每一天去做回归，过滤出的tempdata，是某一天的所有股票的，'stkID'不同
    tempdata = comnbinedfactor.loc[comnbinedfactor['trade_date'] == d, :]
    # MV:MarketValue:流通市值，
    # 用每天的横截面，算一个每个股票和流通市值线性表达后的残差，每天每个股票1个残差
    model = sm.OLS(np.array(tempdata.CLV), np.array(tempdata.MVfactor_std))
    results = model.fit()
    data = np.append(data, results.resid)

CLVneutralizedfactor = pd.DataFrame(data)
CLVneutralizedfactor.reset_index()
comnbinedfactor.reset_index()
CLVneutralizedfactor['trade_date'] = comnbinedfactor.trade_date
CLVneutralizedfactor['stkID'] = comnbinedfactor.stkID
CLVneutralizedfactor = CLVneutralizedfactor.set_index(['trade_date', 'stkID'])  # set联合主键：日期+股票
CLVneutralizedfactor.columns = ['CLV']  # 中性因子，是每个股票每天1个
logger.debug("中性化处理 %d 条数据", len(CLVneutralizedfactor))
assert len(CLVneutralizedfactor) > 0, str(len(CLVneutralizedfactor))


def getForwardReturns(universe, start, end, window, file_name):
    """
    每天都计算一下从当日，到当日+5日后的收益率，所以最后5天没有值
    """
    # 计算个股历史区间前瞻回报率，未来windows天的回报率
    start_time = time.time()
    ret_data = pd.DataFrame()
    for stock in universe:
        data = data_provider.basic(code=stock, start=start, end=end)
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
    logger.debug("计算%d只股票的从%s到%s，%d日回报率,保存到：%s, 耗时%.2f秒", len(universe), start, end, window, file_name,
                 time.time() - start_time)
    return ret_data


window_return = 5
forward_5d_return_data = getForwardReturns(
    universe=universe,
    start=start_date,
    end=end_date,
    window=window_return,
    file_name="data/ForwardReturns_W5_Rtn.csv"
)
forward_5d_return_data = forward_5d_return_data.set_index(['trade_date'])

# """
# IC法
# 1.确定持仓周期，我们这里是5天
# 2.计算每天股票对应的未来调仓周期的收益
# 3.计算未来调仓周期收益率和因子收益率之间的相关系数
# """
# 计算每天的clv收益率和之后5天的股票收益率的秩的相关系数
# 把第二列stkID中的股票值，变成列名，类似于数据透视表，把行数据转成了列，这样做是为了和5日收益率对齐
CLVneutralizedfactor = CLVneutralizedfactor[~CLVneutralizedfactor.index.duplicated()]  # 有重复数据居然，剔除掉
CLVneutralizedfactor_stk = CLVneutralizedfactor.unstack('stkID')['CLV'] # 可以把所以变成列，很神奇，但是，列名多了一个'CLV'
CLVneutralizedfactor_stk = CLVneutralizedfactor_stk.rename_axis(columns=None) # 行变列，参考：https://www.cnblogs.com/traditional/p/11967360.html


# 计算相关系数,每天计算一个
index = 1
ic_data = pd.DataFrame(index=CLVneutralizedfactor_stk.index, columns=['IC', 'pValue'])
for date in ic_data.index:
    """
    每天，找出所有的50只股票的对应的中性化后的因子暴露，是50个数，
    然后，和这一天，这50只股票对应的5日后的股票收益率，也是50个数，
    50个因子暴露：50个5天收益率，做corr相关性（spearmanr）计算，
    得到相关性系数，1个值和对应的1个p_value
    """
    #
    neutralized_factor_expose = CLVneutralizedfactor_stk.loc[date]  # 得到日期的clv因子暴露，20只股票的
    return_5_days = forward_5d_return_data.loc[date]  # 得到日期的5天后的收益率

    corr = pd.DataFrame(neutralized_factor_expose)
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
    # print("finish Rank_IC/spearman for date: %r, %d/%d" % (date,index,len(ic_data)))

logger.debug("计算出 %d 个相关系数（每日1个）", len(ic_data))

# 给每天的CLV,5日收益率,秩相关系数做图
ic_data = ic_data.dropna()
logger.debug("Drop NAN后，剩余 %d 个相关系数", len(ic_data))

logger.debug("IC 均值：%.4f" % ic_data['IC'].mean())
logger.debug("IC 中位数：%.4f" % ic_data['IC'].median())
logger.debug("IC %d个>0, %d个<0" % (len(ic_data[ic_data.IC > 0]), len(ic_data[ic_data.IC < 0])))

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
ax1.set_title("CLV and five days interest rank corr", fontproperties=font, fontsize=16)
ax1.grid()
plt.savefig("debug/CLV和5日收益率的相关性.jpg")

# 计算IC方法二
forward_5d_return_datastack = forward_5d_return_data.stack()
combineMatrix = pd.concat([CLVfactorstack, forward_5d_return_datastack], axis=1, join='inner')
combineMatrix.columns = ['CLV', 'FiveDayfwdRtn']

DayIC = combineMatrix.groupby(level='trade_date').corr(method='spearman')
DayIC = DayIC.reset_index().drop(['level_1'], axis=1)
DayIC = pd.DataFrame(DayIC.loc[DayIC.CLV != 1, 'CLV'])
DayIC.columns = ['IC']

logger.debug("IC mean：%.4f" , DayIC.mean())
logger.debug("IC median：%.4f" , DayIC.median())
logger.debug("IC %d > 0, %d < 0" , len(DayIC[DayIC.IC > 0]), len(DayIC[DayIC.IC < 0]))

# # 回归法
# 1.首先将因子和未来收益率在界面上对齐（日期、代码）
# 2.将未来的收益率作为因变量，因子作为自变量，回归计算出来的系数作为因子收益率
# 3.计算因子收益率的t值等相关统计量

combineMatrix1 = combineMatrix.reset_index()
combineMatrix1.columns = ['trade_date', 'stockID', 'CLV', 'FiveDayfwdRtn']

# 按天回归，回归系数作为因子收益率
unidate = comnbinedfactor.reset_index().trade_date.drop_duplicates()
unidate = list(unidate)
CLVFactorRtn = pd.DataFrame(columns=['CLVfactorRtn', 't_values'], index=unidate)

for d in unidate:
    tempdata = combineMatrix1.loc[combineMatrix1['trade_date'] == d, :]
    tempdata = tempdata.dropna()
    if len(tempdata) > 0:
        model = sm.OLS(np.array(tempdata.FiveDayfwdRtn),
                       np.array(tempdata.CLV))
        results = model.fit()
        CLVFactorRtn.loc[d, 'CLVfactorRtn'] = results.params[0]
        CLVFactorRtn.loc[d, 't_values'] = results.tvalues[0]

"""
回归法因子检测
1.计算t值绝对值的均值，看t值是不是显著不为0，有效性是>2
2.t值绝对值大于2的比例-稳定性(比例大于40%)
3.计算因子收益率的时间序列上的t值,是不是显著不为0 -- 风险因子？alpha因子？
"""
# 1.计算t值绝对值的均值，看t值是不是显著不为0，--- 有效性
print("t_value abs value mean:" % (CLVFactorRtn.t_values.abs().mean()))
# 2.t值绝对值序列大于2的比例 --- 稳定性
print("positive IC percent: %.2f" % len(CLVFactorRtn[CLVFactorRtn.t_values.abs() > 2] / float(len(CLVFactorRtn))))
# 3.计算因子收益率的时间序列上的t值,是不是显著不为0 -- 风险因子？alpha因子？
print("factor interest mean: %.4f" % CLVFactorRtn.CLVfactorRtn.mean())
print("factor interst std: %.4f" % CLVFactorRtn.CLVfactorRtn.std())
print("factor interst sharp: %.4f" % (
        CLVFactorRtn.CLVfactorRtn.mean().item() / (CLVFactorRtn.CLVfactorRtn.std().item() + 0.0000001)))

plt.clf()
from matplotlib.font_manager import FontProperties
font = FontProperties()
# 画图
fig = plt.figure(figsize=(16, 6))
ax1 = fig.add_subplot(111)
lns1 = ax1.plot(np.array(CLVFactorRtn.CLVfactorRtn.cumsum()), label='IC')
lns = lns1
labs = [l.get_label() for l in lns]
ax1.legend(lns, labs,
           bbox_to_anchor=[0.5, 0.1],
           loc='best',
           ncol=3,
           mode='',
           borderaxespad=0.,
           fontsize=12)
ax1.set_xlabel("return intesest", fontproperties=font, fontsize=16)
ax1.set_ylabel("date", fontproperties=font, fontsize=16)
ax1.set_title("CLV return accumulation", fontproperties=font, fontsize=16)
ax1.grid()
plt.savefig("debug/CLV因子累计收益率.jpg")


"""
这个有正、有负，说明其是风险因子，而不是alpha因子，
"""

# In[ ]:

# 分层法检测

n_quantile = 5
# 统计十分位数
cols_mean = [i + 1 for i in range(n_quantile)]
cols = cols_mean

excess_returns_means = pd.DataFrame(index=CLVfactor.index, columns=cols)




# 计算ILLIQ分组的超额收益平均值
# excess_returns_means，是每天一个数，各个5分位的收益率均值（减去了总的平均收益率）
for date in excess_returns_means.index:
    qt_mean_results = []


    tmp_CLV = CLVfactor.loc[date].dropna() # 删除clv中的nan
    tmp_return = forward_5d_return_data.loc[date].dropna() # 删除5日回报率的nan
    tmp_return_mean = tmp_return.mean() # 5日回报率的均值

    pct_quantiles = 1.0 / n_quantile # n_quantile = 5
    for i in range(n_quantile):
        down = tmp_CLV.quantile(pct_quantiles * i) # quantile - 分位数
        up = tmp_CLV.quantile(pct_quantiles * (i + 1))
        i_quantile_index = tmp_CLV[(tmp_CLV <= up) & (tmp_CLV > down)].index # 在clv中找到对应分位数的股票代码
        if not i_quantile_index.isin(tmp_return.index).all():
            logger.warning("Key[%s]不在当前5日汇报率中：%r",i_quantile_index,tmp_return)
            continue
        mean_tmp = tmp_return.loc[i_quantile_index].mean() - tmp_return_mean # 计算这些股票的5天收益率的平均值 - 总体均值
        qt_mean_results.append(mean_tmp)

    if len(qt_mean_results)>0:
        excess_returns_means.loc[date] = qt_mean_results

excess_returns_means.dropna(inplace=True)
excess_returns_means.tail()
logger.debug("一共耗时 ： %.2f 秒" , (time.time() - start_time))

# 画图
plt.clf()
from matplotlib.font_manager import FontProperties
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
ax1.set_title("CLV factor return interest", fontproperties=font, fontsize=16)
ax1.grid()
plt.savefig("debug/CLV5分位股票收益率.jpg")


logger.debug("一 共耗时 ： %.2f 秒" % (time.time() - start_time))

# 使用rqalpha来替代优矿的框架


# 例子中的使用的优矿的回测框架，不够目前不可以用了，都闭源了
# 下面我们使用有矿里面的回测模块使用因子讲股票分组回测

# 可编辑部分和strategy模式一样，其余部分按本例代码编写即可

#---- 回测参数，可编辑 ----
start = '20190101'
end = '20211201'
benchmark = 'ZZ500'             # 策略参考基准
universe = set_universe('HS300')# 股票池
start = '20200101'
end = '20211201'
benchmark = 'ZZ500'  # 策略参考基准
universe = set_universe('HS300')

capital_base = 100000  # 投资资金
freq = 'd'  # 使用日线进行回测
refresh_rate = 5  # 调仓频率, 表示执行handle_data的时间间隔

CLV_dates = CLVfactor.index.values

# 把回测参数封装到SimulationParameters中，供quick_backtest使用
sim_params = quartz.SimulationParameters(start, end, benchmark, universe, capital_base)
# 获取回测行情数据
idxmap, data = quartz.get_backtest_data(sim_params)
# 运行结果
results_illiq = {}

# 调整参数(选取股票的ILLIQ因子五分位数，进行快速回测
for quantile_five in range(1, 6):

    # ---- 策略逻辑部分 ----
    commission = Commission(0.0002, 0.0002)


    def initialize(account):
        pass


    def handle_data(account):  # 单个交易日买入卖出
        pre_date = account.previous_date.strftime("%Y-%m-%d")
        if pre_date not in CLV_dates:  # 只在计算过ILLIQ因子的交易日调仓
            return

        # 拿取调仓日前一个交易日的CLV因子，并按照相应的无分位选择股票
        pre_illiq = CLVfactor.loc[pre_date]
        pre_illiq = pre_illiq.dropna()

        pre_illiq_min = pre_illiq.quantile((quantile_five - 1) * 0.2)
        pre_illiq_max = pre_illiq.quantile(quantile_five * 0.2)
        my_univ = pre_illiq[pre_illiq >= pre_illiq_min][pre_illiq < pre_illiq_max].index.values

        # 调仓逻辑
        univ = [x for x in my_univ if x in account.universe]

        # 不在股票池，清仓
        for stock in account.valid_secpos:
            if stock not in univ:
                order_to(stock, 0)
        # 在目标股票池中，等权买入
        for stock in univ:
            order_pct_to(stock, 1.01 / len(univ))


    # 把回测逻辑封装到 TradeStrategy中，供quick_backtest调用
    strategy = quartz.TradingStrategy(initialize, handle_data)
    # 回测部分
    bt, acct = quartz.quick_backtest(sim_params, strategy, idxmap, data, refresh_rate, commission)

    # 对于回测的结果，可以通过 perf_parse 计算风险指标
    perf = quartz.perf_parse(bt, acct)

    tmp = {}
    tmp['bt'] = bt
    tmp['annualized_return'] = perf['annualized_return']
    tmp['volatility'] = perf['volatility']
    tmp['max_drawdown'] = perf['max_drawdown']
    tmp['alpha'] = perf['alpha']
    tmp['beta'] = perf['beta']
    tmp['sharp'] = perf['sharp']
    tmp['information_ratio'] = perf['information_ratio']

    results_illiq[quantile_five] = tmp
