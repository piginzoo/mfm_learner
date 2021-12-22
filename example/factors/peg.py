"""
参考：
- https://zhuanlan.zhihu.com/p/29144485
- https://www.joinquant.com/help/api/help#factor_values:%E6%88%90%E9%95%BF%E5%9B%A0%E5%AD%90
- https://www.joinquant.com/view/community/detail/087af3a4e27c600ed855cb0c1d0fdfed
在时间序列上，PEG因子的暴露度相对其他因子较为稳定，在近一年表现出较强的趋势性
市盈率相对盈利增长比率PEG = PE / (归母公司净利润(TTM)增长率 * 100) # 如果 PE 或 增长率为负，则为 nan
"""


# 计算股票的PEG值
# 输入：context(见API)；stock_list为list类型，表示股票池
# 输出：df_PEG为dataframe: index为股票代码，data为相应的PEG值
def get_PEG(context, stock_list):
    # 查询股票池里股票的市盈率，收益增长率
    q_PE_G = query(valuation.code, valuation.pe_ratio, indicator.inc_net_profit_year_on_year).filter(
        valuation.code.in_(stock_list))
    # 得到一个dataframe：包含股票代码、市盈率PE、收益增长率G
    # 默认date = context.current_dt的前一天,使用默认值，避免未来函数，不建议修改
    df_PE_G = get_fundamentals(q_PE_G)

    # 筛选出成长股：删除市盈率或收益增长率为负值的股票
    df_Growth_PE_G = df_PE_G[(df_PE_G.pe_ratio > 0) & (df_PE_G.inc_net_profit_year_on_year > 0)]
    # 去除PE或G值为非数字的股票所在行
    df_Growth_PE_G.dropna()

    # 写法1:会有warning
    df_Growth_PE_G['PEG'] = df_Growth_PE_G['pe_ratio'] / df_Growth_PE_G['inc_net_profit_year_on_year']
    df_PEG = df_Growth_PE_G[['code', 'PEG']]
    df_PEG.set_index('code', inplace=True)

    '''
    # 写法2:原做写法
    Series_PE = df_Growth_PE_G.ix[:,'pe_ratio']  # 得到一个Series：存放股票的市盈率TTM，即PE值
    Series_G = df_Growth_PE_G.ix[:,'inc_net_profit_year_on_year']  # 得到一个Series：存放股票的收益增长率，即G值
    Series_PEG = Series_PE/Series_G  # 得到一个Series：存放股票的PEG值
    Series_PEG.index = df_Growth_PE_G.ix[:,0]  # 将股票与其PEG值对应
    df_PEG = pd.DataFrame(Series_PEG)  # 将Series类型转换成dataframe类型
    '''

    return df_PEG