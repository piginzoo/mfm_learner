import logging

import pandas as pd

from datasource.impl.fields_mapper import MAPPER
from utils import CONF

logger = logging.getLogger(__name__)


def reset_index(factors):
    """把索引设置成[日期+股票代码]的复合索引"""
    factors['datetime'] = pd.to_datetime(factors['datetime'], format=CONF['dateformat'])  # 时间为日期格式，tushare是str
    factors = factors.set_index(['datetime', 'code'])
    return factors


def comply_field_names(df):
    """
    按照 datasource.impl.fields_mapper.MAPPER 中定义的字段映射，对字段进行统一改名
    """
    datasource_type = CONF['datasource']
    column_mapping = MAPPER.get(datasource_type)
    if column_mapping is None: raise ValueError("字段映射无法识别映射类型(即数据类型)：" + datasource_type)
    df = df.rename(columns=column_mapping)
    return df


def load_daily_data(datasource, stock_codes, start_date, end_date):
    df_merge = pd.DataFrame()
    # 每支股票
    for stock_code in stock_codes:
        # 得到日交易数据
        data = datasource.daily(stock_code=stock_code, start_date=start_date, end_date=end_date)
        df_merge = comply_field_names(df_merge)
        data = data.sort_values(['datetime'])
        if df_merge is None:
            df_merge = data
        else:
            df_merge = df_merge.append(data)
        # logger.debug("加载%s~%s的股票[%s]的 %d 条CLV数据", start_date, end_date, stock_code, len(data))

    logger.debug("一共加载%s~%s %d条 CLV 数据", start_date, end_date, len(df_merge))

    factors = df_merge[['datetime', 'code', 'CLV']]

    return reset_index(factors)
