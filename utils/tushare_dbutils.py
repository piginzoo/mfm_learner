"""
这个使用B站UP主的数据源导入后的mysql进行访问
"""
import logging

import pandas as pd

from utils import utils, tushare_utils

logger = logging.getLogger(__name__)


# 返回每日行情数据，不限字段
def daliy_one(stock_code, start_date, end_date, db_engine):
    df = pd.read_sql(
        f'select * from daily_hfq where ts_code="{stock_code}" and trade_date>="{start_date}" and trade_date<="{end_date}"',
        db_engine)
    return df


def daily(stock_code, start_date, end_date):
    db_engine = utils.connect_db()
    if type(stock_code) == list:
        logger.debug("获取多只股票的交易数据：%r", ",".join(stock_code))
        df_all = None
        for stock in stock_code:
            df_daily = daliy_one(stock, start_date, end_date, db_engine)
            if df_all is None:
                df_all = df_daily
            else:
                df_all = df_all.append(df_daily)
        logger.debug("获取 %s ~ %s 多只股票的交易数据：%d 条", start_date, end_date, len(df_all))
        return df_all
    else:
        df_one = daliy_one(stock_code, start_date, end_date, db_engine)
        logger.debug("获取 %s ~ %s 股票[%s]的交易数据：%d 条", start_date, end_date, stock_code, len(df_one))
        return df_one


# 返回每日的其他信息，主要是市值啥的
def daily_basic(stock_code, start_date, end_date):
    db_engine = utils.connect_db()
    df = pd.read_sql(
        f'select * from daily_basic \
        where ts_code="{stock_code}" and trade_date>="{start_date}" and trade_date<="{end_date}"', db_engine)
    return df


# 指数日线行情
def index_daily(index_code, start_date, end_date):
    return tushare_utils.index_daily(index_code, start_date, end_date)


# 返回指数包含的股票
def index_weight(index_code, start_date):
    return tushare_utils.index_weight(index_code, start_date)


# 获得财务数据
def fina_indicator(stock_code, start_date, end_date):
    db_engine = utils.connect_db()
    df = pd.read_sql(
        f'select * from fina_indicator \
        where ts_code="{stock_code}" and ann_date>="{start_date}" and ann_date<="{end_date}"', db_engine)
    return df


# python -m utils.tushare_dbutils
if __name__ == '__main__':
    ts_code = '000001.SZ'
    start_date = '20180101'
    end_date = '20181011'
    print(daily(ts_code, start_date, end_date))
    print(daily_basic(ts_code, start_date, end_date))
    print(index_daily(ts_code, start_date, end_date))
    print(fina_indicator(ts_code, start_date, end_date))
