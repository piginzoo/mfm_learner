import logging

import pandas as pd

from datasource import datasource_factory as ds_factory
from utils import CONF

logger = logging.getLogger(__name__)


def reset_index(factors):
    """把索引设置成[日期+股票代码]的复合索引"""
    assert 'datetime' in factors.columns, factors.columns
    assert 'code' in factors.columns, factors.columns

    factors['datetime'] = to_datetime(factors['datetime'])
    factors = factors.set_index(['datetime', 'code'])
    return factors


def to_datetime(series):
    return pd.to_datetime(series, format=CONF['dateformat'])  # 时间为日期格式，tushare是str


def load_daily_data(datasource, stock_codes, start_date, end_date):
    df_merge = pd.DataFrame()
    # 每支股票
    for stock_code in stock_codes:
        # 得到日交易数据
        data = datasource.daily(stock_code=stock_code, start_date=start_date, end_date=end_date)
        if df_merge is None:
            df_merge = data
        else:
            df_merge = df_merge.append(data)
        # logger.debug("加载%s~%s的股票[%s]的 %d 条daliy数据", start_date, end_date, stock_code, len(data))
    logger.debug("一共加载%s~%s %d条 CLV 数据", start_date, end_date, len(df_merge))
    return df_merge


def compile_industry(df_industry):
    """
    把行业列（文字）转换成统一的行业码
    如 "家用电器" => '330000'
    ----------
    index_classify结果：
    index_code  industry_name    level  industry_code is_pub parent_code
    801024.SI          采掘服务    L2        210400   None      210000
    801035.SI          石油化工    L2        220100   None      220000
    801033.SI          化学原料    L2        220200   None      220000
    靠，tushare返回的是中文字符串，不是，申万的行业代码，所以，我得自己处理了。
    ----------
    df_industry: 股票对应的行业中文名，3列[code, date, industry]
    """
    df_industries = ds_factory.get().index_classify()

    def find_industry_code(chinese_name):

        # import pdb;pdb.set_trace()
        found_rows = df_industries.loc[df_industries['industry_name'] == chinese_name]

        for _, row in found_rows.iterrows():
            r = __extract_industry_code(row)
            if r: return r

        r = __get_possible(df_industries, chinese_name)
        if r is None:
            ValueError('无法找到 [' + chinese_name + "] 对应的行业代码")
        return r

    def __extract_industry_code(row):
        if row.level == 'L1': return row.industry_code
        if row.level == 'L2': return row.parent_code
        if row.level == 'L3':  # 假设一定能找到
            assert len(df_industries.loc[df_industries['industry_code'] == row.parent_code]) > 0
            return df_industries.loc[df_industries['industry_code'] == row.parent_code][0].parent_code
        raise None

    def __get_possible(df_industries, chinese_name):
        from Levenshtein import distance

        df_industries['distance'] = df_industries['industry_name'].apply(
            lambda x: distance(x, chinese_name)
        )
        first_row = df_industries.sort_values('distance').iloc[0]
        code = __extract_industry_code(first_row)

        # logger.debug("行业纠错：%s=>%s:%s",chinese_name,first_row['industry_name'],code)

        return code

    # 用中文名列，生成，申万的行业代码列
    df_industry['industry'] = df_industry['industry'].apply(find_industry_code)

    return df_industry[['industry']]
