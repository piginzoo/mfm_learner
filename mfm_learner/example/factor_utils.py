import logging

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from sklearn import preprocessing

from mfm_learner.datasource import datasource_utils, datasource_factory
from mfm_learner.example.factors.factor import Factor
from mfm_learner.utils import utils, dynamic_loader, logging_time, db_utils

logger = logging.getLogger(__name__)


def winsorize(se):
    assert type(se) == Series
    """
    缩尾处理
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


def standardize(se):
    """标准化"""
    assert type(se) == Series

    se_std = se.std()
    se_mean = se.mean()
    return (se - se_mean) / se_std


def fill_nan(se):
    assert type(se) == Series, type(se)
    return se.fillna(se.dropna().mean())


@logging_time
def preprocess(factors):
    # 如果就1列，就转成Series，方便处理
    if type(factors) == DataFrame and len(factors.columns) == 1:
        factors = factors.iloc[:, 0]

    """
    做标准化处理，都是基于截面的，即某一天，多只股票，之间的值填充nan，去极值，标准化
    """
    factors = factors.groupby(level='datetime').apply(fill_nan)  # 填充NAN，以截面的均值来填充nan
    factors = factors.groupby(level='datetime').apply(winsorize)  # 去极值，把分数为97.5%和2.5%之外的异常值替换成分位数值
    factors = factors.groupby(level='datetime').apply(standardize)  # 标准化（减去均值除以方差）
    logger.debug("规范化预处理，%d行", len(factors))
    return factors


def to_panel_of_stock_columns(df):
    """
    从[日期|股票]+值的Series，转换成，[日期|股票1|股票2|...|股票n]的panel数据
    这个格式是jaqs_fxdayu合成因子的api要求的格式
    从
        --------------------------------
        <       index       >   value
        date        stock
        2012-06-24  000001.SH   0.1234
        2012-06-27  000001.SH   0.5678
        ...         ...         ...
        2012-06-24  000002.SH   0.5678
        ...         ...         ...
        --------------------------------
    转成
        ---------------------------------------------------------------------------------------------
        　　　　　　　000001.SZ	000002.SZ	000008.SZ	000009.SZ	000027.SZ	000039.SZ	000060.SZ
        date
        2016-06-24	0.165260	0.002198	0.085632	-0.078074	0.173832	0.214377	0.068445
        2016-06-27	0.165537	0.003583	0.063299	-0.048674	0.180890	0.202724	0.081748
        2016-06-28	0.135215	0.010403	0.059038	-0.034879	0.111691	0.122554	0.042489
        2016-06-29	0.068774	0.019848	0.058476	-0.049971	0.042805	0.053339	0.079592
        2016-06-30	0.039431	0.012271	0.037432	-0.027272	0.010902	0.077293	-0.050667
        ---------------------------------------------------------------------------------------------

    的Panel数据
    """
    assert len(df) > 0, df
    if type(df) == DataFrame:
        df = df.iloc[:, 0]  # 把dataframe转成series，这样做的缘故是，unstack的时候，可以避免复合列名，如 ['clv','003859.SH']
    assert type(df) == Series, type(df)
    assert len(df.index.names) == 2, df
    df = df.unstack()
    return df


def fill_inf(df):
    return df.replace([np.inf, -np.inf], np.nan)


def zscore(df):
    """使用sklean的方法，归一化"""
    df.iloc[:, 0] = preprocessing.scale(df[:, 0])  # z-score 规范化
    return df


def _mask_df(df, mask):
    mask = mask.astype(bool)
    df[mask] = np.nan
    return df


def _mask_non_index_member(df, index_member=None):
    if index_member is not None:
        index_member = index_member.astype(bool)
        return _mask_df(df, ~index_member)
    return df


def rank_with_mask(df, axis=1, mask=None, normalize=False, method='min'):
    """

    Parameters
    ----------
    df : pd.DataFrame
    axis : {0, 1}
    mask : pd.DataFrame
    normalize : bool
    method : {'min', 'average', 'max', 'dense'}

    Returns
    -------
    pd.DataFrame

    Notes
    -----
    If calculate rank, use 'min' method by default;
    If normalize, result will range in [0.0, 1.0]

    """
    not_nan_mask = (~df.isnull())

    if mask is None:
        mask = not_nan_mask
    else:
        mask = np.logical_and(not_nan_mask, mask)

    rank = df[mask].rank(axis=axis, na_option='keep', method=method)

    if normalize:
        dividend = rank.max(axis=axis)
        SUB = 1
        # for dividend = 1, do not subtract 1, otherwise there will be NaN
        dividend.loc[dividend > SUB] = dividend.loc[dividend > SUB] - SUB
        rank = rank.sub(SUB).div(dividend, axis=(1 - axis))
    return rank


def check_factor_format(df, index_type='date'):
    """
    index_type: 索引烈性，有两种格式：'date','date_code'
    烦死了，每次因子格式都不知道对不对，我写个函数来强制检查
    因子界面表示，有两种格式：
        格式1：索引是[date,code], 列是[value] ===> 对应 index_type='date'
        格式2：索引是[date], 列是[code1,code2,code3] ====> 对应 index_type='date_code'
    """
    index_num = 1 if index_type == 'date' else 2
    return len(df.index.names) == index_num


# 将因子值加一个极小的扰动项,用于对quantile做区分
def get_disturbed_factor(factor_df):
    """
    将因子值加一个极小的扰动项,用于对quantile区分
    :param factor_df: 因子值 (pandas.Dataframe类型),index为datetime, colunms为股票代码。
                      形如:
                                  　AAPL	　　　     BA	　　　CMG	　　   DAL	      LULU	　　
                        date
                        2016-06-24	0.165260	0.002198	0.085632	-0.078074	0.173832
                        2016-06-27	0.165537	0.003583	0.063299	-0.048674	0.180890
                        2016-06-28	0.135215	0.010403	0.059038	-0.034879	0.111691
                        2016-06-29	0.068774	0.019848	0.058476	-0.049971	0.042805
                        2016-06-30	0.039431	0.012271	0.037432	-0.027272	0.010902

    :return: 重构后的因子值,每个值加了一个极小的扰动项。
    """
    return factor_df + np.random.random(factor_df.shape) / 1000000000


# 检查数据中是否含有任何缺失值
def is_any_nan(df):
    return df.isnull().values.any()


# 参考：https://mp.weixin.qq.com/s/WW3up8JwCIx0PwkpSx-oyg
def nan_sum(df):
    # 查看每列数据缺失值情况
    df.isnull().sum()


def nan_count(df, fields):
    # df[fields].
    pass


def pct_chg(prices, days=1):
    """
    计算收益率
    """
    return (prices.shift(-days) - prices) / prices  # 向后错days天


# 行业、市值中性化 - 对Dataframe数据，参考自jaqs_fxdayu代码
def neutralize(factor_df, df_stock_basic, df_mv):
    """
    :param factor_df:
    :param df_stock_basic:    股票的基本信息，包含了行业

    :return:
    """

    """
    对因子做行业、市值中性化，实际上是用市值来来做回归。
    因为有很多天数据，所以，这个F和X是一个[Days]的一个向量，回归出的e，是一个[days]的残差向量
    注意，在做行业中性化的时候，F实际上不是一个向量了，而是是一个行业宽度的一个矩阵[days,industies]，但是残差还是一个[days]向量
    -------------
    X = w * F + e
    X就是市值
    F为需要被市值中性化的因子
    e，就是去市值中性化后的结果，即，回归残差

    :param index_member:
    :param group:　行业分类(pandas.Dataframe类型),index为datetime, colunms为股票代码
                   行业分类（也可以是其他分组方式）。日期为索引,证券品种为columns的二维表格,对应每一个品种在某期所属的分类
                        date        code        industry
                        2016-06-24	000123.SH   23
                        2016-06-24	000124.SH   23
                        2016-06-24	000125.SH   22
                        2016-06-24	000126.SH   22
    :param factor_df: 因子值 (pandas.Dataframe类型),index为datetime, colunms为股票代码。
                      形如:
                        code       　AAPL	　　　     BA	　　　CMG	　　   DAL	      LULU	　　
                        date
                        2016-06-24	0.165260	0.002198	0.085632	-0.078074	0.173832
                        2016-06-27	0.165537	0.003583	0.063299	-0.048674	0.180890
                        2016-06-28	0.135215	0.010403	0.059038	-0.034879	0.111691
                        2016-06-29	0.068774	0.019848	0.058476	-0.049971	0.042805
                        2016-06-30	0.039431	0.012271	0.037432	-0.027272	0.010902
    :param float_mv: 流通市值因子(pandas.Dataframe类型),index为datetime, colunms为股票代码．为空则不进行市值中性化
    :return: 中性化后的因子值(pandas.Dataframe类型),index为datetime, colunms为股票代码。
    """

    def _get_stock_info(factor_df):
        """从索引中剥离开始日期、结束日期、所有的股票"""
        assert len(factor_df.index.names) == 2 and factor_df.index.names[0] == 'datetime', factor_df.index.names
        assert len(factor_df.index.names) == 2 and factor_df.index.names[1] == 'code', factor_df.index.names
        stock_codes = factor_df.index.levels[1].tolist()
        dates = factor_df.index.levels[0]
        dates = dates.sort_values()
        start_date = utils.date2str(dates[0])
        end_date = utils.date2str(dates[-1])
        return stock_codes, start_date, end_date

    def _ols_by_numpy(x, y):
        # least-squares，最小二乘，m是回归系数：y = m * x + resid
        m = np.linalg.lstsq(x, y)[0]
        # 得到残差
        resid = y - (x @ m)
        return resid

    def _generate_cross_sectional_residual(data):
        """
        就是把industry，变成one-hot，然后和signal（因子值）做多元回归，求残差
        --------------------------------------------------
        date        code        signal          industry
        2016-06-24	000123.SH   1.1             23
        2016-06-24	000124.SH   1.2             23
        2016-06-24	000125.SH   1.3             22
        2016-06-24	000126.SH   1.4             22
        :param data:
        :return:
        """
        for _, X in data.groupby(level=0):
            # signal就是因子值
            signal = X.pop("signal")  # pop这写法骚啊，就是单独取一列的意思，和X['pop']一个意思，不过，还包含了删除这列
            """
            pd.get_dummies(['A','B','A','C'])
               A  B  C
            0  1  0  0
            1  0  1  0
            2  1  0  0
            3  0  0  1
            X可能会有两列[signal,style(mv)]，也可能只有1列[signal]，
            最终回归的时候，signal会变成one-hot，
            """
            X = pd.concat([X, pd.get_dummies(X.pop("industry"))], axis=1)
            """
            signal(factor value因子值) = w1*0 + ... wi*1 + ... wn*0 + e
            我们用行业的one-hot作为x，去多元回归因子值y，剩余的残差e，就是我们需要的
            """
            signal = pd.Series(_ols_by_numpy(X.values, signal), index=signal.index, name=signal.name)
            # 靠，为何要用yield，看着晕，其实就是每行都处理的意思
            yield signal

    stock_codes, start_date, end_date = _get_stock_info(factor_df)

    df_factor_temp = DataFrame(factor_df)  # 防止他是Series
    assert check_factor_format(factor_df, index_type='date_code')
    df_factor_temp = df_factor_temp.reset_index()

    # 行业中性化
    assert 'code' in df_factor_temp, df_factor_temp
    assert 'code' in df_stock_basic, df_stock_basic
    # 把因子数据，和，股票基础数据（包含行业）做merge
    df_factor_temp = df_factor_temp.merge(df_stock_basic[['code', 'industry']],
                                          on="code")  # stocks_info行太少，需要和factors做merge
    df_factor_temp = df_factor_temp.set_index(['datetime', 'code'])

    # 这步很重要，行业数据是中文的（吐槽tushare），我必须要转成申万的行业代码
    df_industry = datasource_utils.compile_industry(df_factor_temp['industry'])

    data = []

    # 准备因子数据
    assert check_factor_format(factor_df, index_type='date_code')
    data.append(utils.dataframe2series(factor_df).rename("signal"))

    # 获取对数流动市值，并去极值、标准化。市值类因子不需进行这一步
    df_mv = preprocess(df_mv)
    data.append(df_mv)

    # 行业中性化处理
    assert check_factor_format(df_industry, index_type='date_code')
    industry_standard = utils.dataframe2series(df_industry).rename("industry")
    data.append(industry_standard)

    data = pd.concat(data, axis=1).dropna()  # 按列(axis=1)合并，其实是贴到最后一列上，索引要相同，都是 [datetime|code]
    # 做行业中性化 = one-hot行业回归后的残差
    residuals = pd.concat(_generate_cross_sectional_residual(data))

    """"
    中性化结果：
    datetime  code
    20200102  300433.SZ   -5.551115e-17
              300498.SZ    0.000000e+00
              600000.SH    1.110223e-16
    """
    return residuals


def get_factor_names():
    class_dict = dynamic_loader.dynamic_instantiation("example.factors", Factor)
    names = []
    for _, cls in class_dict:
        factor_name = cls().name()
        if type(factor_name) == list:
            names += factor_name
        else:
            names.append(factor_name)
    return names


def __get_one_factor(datasource, name, stock_codes, start_date, end_date):
    df = datasource.get_factor(name, stock_codes, start_date, end_date)
    if df is None: return None
    df = datasource_utils.reset_index(df)
    return df


def get_factor(name, stock_codes, start_date, end_date):
    """
    返回因子数据： DataFrame（Index=[datetime,code], Value=[因子值]）
    如果是多个，就返回List<DataFrame>
    :param name:
    :param stock_codes:
    :param start_date:
    :param end_date:
    :return:
    """

    datasource = datasource_factory.create('database')  # 因子只可能在数据库中，这里写死数据源类型

    if type(name) == list:
        df_factors = [__get_one_factor(datasource, __name, stock_codes, start_date, end_date) for __name in name]
        logger.debug("加载了%d个因子数据：%r", len(name), name)
        return df_factors
    else:
        df_factor = __get_one_factor(datasource, name, stock_codes, start_date, end_date)
        logger.debug("加载了因子[%s]数据，%d行", name, len(df_factor))
        return df_factor


def get_factor_dict(factor_names, stock_codes, start_date, end_date):
    df_factors = get_factor(factor_names, stock_codes, start_date, end_date)
    factor_dict = {}
    logger.debug("开始加载因子：%r", factor_names)
    for df_factor, factor_name in zip(df_factors, factor_names):
        factor_dict[factor_name] = df_factor
        logger.debug("加载了因子[%s] %d条", factor_name, len(df_factor))
    return factor_dict


def get_factor_synthesis(name, stock_codes, start_date, end_date):
    """直接替换旧数据"""
    engine = utils.connect_db()

    stock_codes = db_utils.list_to_sql_format(stock_codes)

    sql = f"""
        select datetime,code,value
        from factor_synthesis 
        where datetime>=\'{start_date}\' and 
              datetime<=\'{end_date}\' and
              code in ({stock_codes}) and
              name = \'{name}\'
    """

    df = pd.read_sql(sql, engine)
    df = datasource_utils.reset_index(df)  # 为了和单因子统一，也做这个处理，即用datetime和code做联合index
    logger.debug("从表[%s]加载合成因子[%s] %d条", 'factor_synthesis', name, len(df))

    return df


def __factor2db_one(name, df):
    """直接替换旧数据"""
    engine = utils.connect_db()
    df.to_sql(f'factor_{name}', engine, index=False, if_exists='replace')  # replace 替换掉旧的
    logger.debug("保存因子到数据库：表[%s]", f'factor_{name}')


def factor2db(name, factor):
    if type(name) == list:
        return [__factor2db_one(__name, __factor) for __name, __factor in zip(name, factor)]
    else:
        return __factor2db_one(name, factor)


def factor_synthesis2db(name, desc, df_factor):
    engine = utils.connect_db()

    df_factor['name'] = name
    df_factor['desc'] = desc
    # df_factor = datasource_utils.date2str(df_factor,'datetime') # 把日期列变成字符串

    # 先删除旧的因子分析结果
    if db_utils.is_table_exist(engine, "factor_synthesis"):
        db_utils.run_sql(engine, f"delete from factor_synthesis where name='{name}'")

    df_factor.to_sql(f'factor_synthesis', engine, index=False, if_exists='append')  # replace 替换掉旧的
    logger.debug("保存合成因子到数据库：表[%s] ，名称:%s, %d行", 'factor_synthesis', name, len(df_factor))


def handle_finance_ttm(stock_codes,
                       df_finance,
                       trade_dates,
                       col_name_value,
                       col_name_finance_date='end_date'):
    """
    处理TTM：以当天为基准，向前滚动12个月的数据，
    用于处理类ROE_TTM数据，当然不限于ROE，只要是同样逻辑的都支持。

    @:param finance_date  - 真正的财报定义的日期，如3.30、6.30、9.30、12.31

    ts_code    ann_date  end_date      roe
    600000.SH  20201031  20200930   7.9413
    600000.SH  20200829  20200630   5.1763
    600000.SH  20200425  20200331   3.0746
    600000.SH  20200425  20191231  11.4901
    600000.SH  20191030  20190930   9.5587 <----- 2019.8.1日可回溯到的日期
    600000.SH  20190824  20190630   6.6587
    600000.SH  20190430  20190331   3.4284
    600000.SH  20190326  20181231  12.4674

    处理方法：
    比如我要填充每一天的ROE_TTM，就要根据当前的日期，回溯到一个可用的ann_date（发布日期），
    然后以这个日期，作为可用数据，计算ROE_TTM。
    比如当前日是2019.8.1日，回溯到2019.10.30(ann_date)日发布的3季报（end_date=20190930, 0930结尾为3季报），
    然后，我们的计算方法就是，用3季报，加上去年的年报，减去去年的3季报。
    -----
    所以，我们抽象一下，所有的规则如下：
    - 如果是回溯到年报，直接用年报作为TTM
    - 如果回溯到1季报、半年报、3季报，就用其 + 去年的年报 - 去年起对应的xxx报的数据，这样粗暴的公式，是为了简单
    """

    # 提取，发布日期，股票，财务日期，财务指标 ，4列
    df_finance = df_finance[['datetime', 'code', col_name_finance_date, col_name_value]]
    # 剔除Nan
    df_finance.dropna(inplace=True)

    # 对时间，升序排列
    df_finance.sort_values('datetime', inplace=True)

    # 未来的，ttm列名
    ttm_col_name_value = col_name_value + "_ttm"

    # 创建空的结果DataFrame
    df_factor = pd.DataFrame(columns=['datetime', 'code', col_name_value])

    # 返回的数据，应该是交易日数据；一只一只股票的处理
    for stock_code in stock_codes:

        # 过滤一只股票
        df_stock_finance = df_finance[df_finance['code'] == stock_code]

        logger.debug("处理股票[%s]财务数据%d条", stock_code, len(df_stock_finance))

        # 处理每一天
        for the_date in trade_dates:

            # 找到最后发布的行：按照当前日作为最后一天，去反向搜索发布日在当前日之前的数据，取最后一条，就是最后发布的数据
            series_last_one = df_stock_finance[df_stock_finance['datetime'] <= the_date].iloc[-1]

            # 取出最后发布的财务日期
            finance_date = series_last_one[col_name_finance_date]

            # 取出最后发布的财务日期对应的指标值
            current_period_value = series_last_one[col_name_value]

            # 如果这条财务数据是年报数据
            if finance_date.endswith("1231"):
                # 直接用这条数据了
                value = current_period_value
                # logger.debug("财务日[%s]是年报数据，使用年报指标[%.2f]作为当日指标", finance_date, value)
            else:
                # 如果回溯到1季报、半年报、3季报，就用其 + 去年的年报 - 去年起对应的xxx报的数据，这样粗暴的公式，是为了简单
                last_year_value = __last_year_value(df_stock_finance, col_name_finance_date, col_name_value,
                                                    finance_date)
                last_year_same_period_value = __last_year_period_value(df_stock_finance, col_name_finance_date,
                                                                       col_name_value, finance_date)
                # 如果去年年报数据为空，或者，也找不到去年的同期的数据，
                if last_year_value is None or last_year_same_period_value is None:
                    value = __calculate_ttm_by_peirod(current_period_value, finance_date)
                    # logger.debug("财务日[%s]是非年报数据，无去年报指标，使用N倍当前指标[%.2f]作为当日指标", finance_date, value)
                else:
                    # 当日指标 = 今年同期 + 年报指标 - 去年同期
                    value = current_period_value + last_year_value - last_year_same_period_value
                    # logger.debug("财务日[%s]是非年报数据，今年同期[%.2f]+年报指标[%.2f]-去年同期[%.2f]=[%.2f]作为当日指标",
                    #              finance_date,
                    #              current_period_value,
                    #              last_year_value,
                    #              last_year_same_period_value,
                    #              value)

            df_factor = df_factor.append(
                {'datetime': the_date,
                 'code': stock_code,
                 col_name_value: current_period_value,
                 ttm_col_name_value: value},
                ignore_index=True)
    logger.debug("生成%d条TTM数据", len(df_factor))
    return df_factor


def __last_year_value(df_stock_finance, finance_date_col_name, value_col_name, current_finance_date):
    last_year_finance_date = current_finance_date[:4] + "1231"
    return __last_year_period_value(df_stock_finance, finance_date_col_name, value_col_name,
                                    current_finance_date=last_year_finance_date)


def __last_year_period_value(df_stock_finance, finance_date_col_name, value_col_name, current_finance_date):
    """获得去年同时期的财务指标"""

    # 获得去年财务年报的时间，20211030=>20201030
    last_year_finance_date = utils.last_year(current_finance_date)
    df = df_stock_finance[df_stock_finance[finance_date_col_name] == last_year_finance_date]
    # assert len(df) == 0 or len(df) == 1, str(df)
    if len(df) == 1: return df[value_col_name].item()
    if len(df) == 0: return None
    logger.warning("记录数超过2条，取第一条：%r",df)
    return df[value_col_name].iloc[0]



def __calculate_ttm_by_peirod(current_period_value, finance_date):
    PERIOD_DEF = {
        '0331': 4,
        '0630': 2,
        '0930': 1.33,
    }

    periods = PERIOD_DEF.get(finance_date[-4:], None)
    if periods is None:
        logger.warning("无法根据财务日期[%s]得到财务的季度间隔数", finance_date)
        return np.nan
    return current_period_value * periods


def handle_finance_fill(datasource,
                        stock_codes,
                        start_date,
                        end_date,
                        finance_index_col_name_value):
    """
    处理财务数据填充，因为财政指标只在年报发布时候提供，所以要填充那些非发布日的数据，
    比如财务数据仅提供了财务报表发表日的的数据，那么我们需要用这个数据去填充其他日子，
    填充原则是，以发布日为基准，当日数据以最后发布日的数据为准，
    算法是用通用日历来填充其他数据，但是，可能某天此股票停盘，无所谓，还是给他算出来，
    实现是，按照日历创建空记录集，然后做左连接，空位用前面的数据补齐
    有个细节，开始的日子需要再之前的财务数据，因此，我只好多query1年前的财务数据来处理，最终在过滤掉之前的数据
    """

    # 需要把提前1年的财务数据和日历都得到
    start_date_1years_ago = utils.last_year(start_date, num=1)
    # 交易日期（包含1年前）
    trade_dates = datasource.trade_cal(start_date_1years_ago, end_date)
    # 财务数据（包含1年前的）
    df_finance = datasource.fina_indicator(stock_codes, start_date_1years_ago, end_date)
    # 提取，发布日期，股票，财务日期，财务指标 ，4列
    df_finance = df_finance[['code', 'datetime', finance_index_col_name_value]]
    # 对时间，升序排列
    df_finance.sort_values('datetime', inplace=True)
    # 创建每个交易日为一行的一个辅助dataframe，用于生成每个股票的交易数据
    df_calender = pd.DataFrame(trade_dates)
    df_calender.columns = ['datetime']
    # 创建空的结果DataFrame，保存最终结果
    df_result = pd.DataFrame(trade_dates, columns=['code', 'datetime', finance_index_col_name_value])
    # 返回的数据，应该是交易日数据；一只一只股票的处理
    for stock_code in stock_codes:
        # 过滤一只股票
        df_stock_finance = df_finance[df_finance['code'] == stock_code]
        logger.debug("处理股票[%s]财务数据%d条", stock_code, len(df_stock_finance))
        # 左连接，交易日（左连接）财务数据，这样，没有的交易日，数据为NAN
        df_join = df_calender.merge(df_stock_finance, how="left", on='datetime')
        # 为防止日期顺序有问题，重新排序
        df_join = df_join.sort_values('datetime')
        # 向下填充nan值，这个是一个神奇的方法，跟Stack Overflow上学的
        # 参考 https://stackoverflow.com/questions/27905295/how-to-replace-nans-by-preceding-or-next-values-in-pandas-dataframe
        df_join = df_join.fillna(method='ffill')
        # 补齐股票代码
        df_join['code'] = stock_code
        # 因为提前了1年的数据，所以，要把这些提前数据过滤掉
        df_join = df_join[df_join.datetime >= start_date]

        # 做一个断言，理论上不应该有nan数据
        nan_sum = df_join[finance_index_col_name_value].isnull().sum()
        assert nan_sum == 0, f"你需要多传一年的财务数据，防止NAN: {nan_sum}行NAN "

        # 合并到总结果中
        df_result = df_result.append(df_join, ignore_index=True)

    return df_result


# python -m mfm_learner.example.factor_utils
if __name__ == '__main__':
    utils.init_logger()
    df = get_factor("clv", "20210101", "20210801")
    print(df.head(3))
    print(len(df))
