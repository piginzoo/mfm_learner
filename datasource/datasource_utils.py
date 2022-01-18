import logging

import pandas as pd
from pandas import DataFrame

from datasource.impl.fields_mapper import MAPPER
from utils import CONF
from datasource import datasource_factory as ds_factory

logger = logging.getLogger(__name__)


def reset_index(factors):
    """把索引设置成[日期+股票代码]的复合索引"""
    factors['datetime'] = pd.to_datetime(factors['datetime'], format=CONF['dateformat'])  # 时间为日期格式，tushare是str
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


def compile_industry(industry_series):
    """
    把行业列（文字）转换成统一的行业码
    如 "家用电器" => '330000'
    ----------
    index_code  industry_name    level  industry_code is_pub parent_code
    801024.SI          采掘服务    L2        210400   None      210000
    801035.SI          石油化工    L2        220100   None      220000
    801033.SI          化学原料    L2        220200   None      220000

    """
    df_industries = ds_factory.get().index_classify()

    def find_industry_code(chinese_name):

        # import pdb;pdb.set_trace()
        found_rows = df_industries.loc[df_industries['industry_name'] == chinese_name]

        if len(found_rows) == 0: raise ValueError('无法找到 [' + chinese_name + "] 对应的行业代码")

        # 如果有1级的，直接返回
        for _,row in found_rows.iterrows():
            if row.level == 'L1': return row.industry_code
            if row.level == 'L2': return row.parent_code

        # 如果是level=3，需回溯
        for _,row in found_rows.iterrows():
            if row.level == 'L3':  # 假设一定能找到
                assert len(df_industries.loc[df_industries['industry_code'] == row.parent_code]) > 0
                return df_industries.loc[df_industries['industry_code'] == row.parent_code][0].parent_code
        raise ValueError('无法找到 [' + chinese_name + "] 对应的行业代码")

    industry_code_series = industry_series.apply(find_industry_code)

    return industry_code_series
