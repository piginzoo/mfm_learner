"""
这个使用B站UP主的数据源导入后的mysql进行访问
"""
import logging
import time

import pandas as pd

from datasource.datasource import DataSource, post_query
from datasource.impl.tushare_datasource import TushareDataSource
from utils import utils, logging_time

logger = logging.getLogger(__name__)


class DatabaseDataSource(DataSource):
    def __init__(self):
        self.db_engine = utils.connect_db()
        self.tushare = TushareDataSource()

    # 返回每日行情数据，不限字段
    def __daliy_one(self, stock_code, start_date, end_date):
        df = pd.read_sql(
            f'select * from daily_hfq where ts_code="{stock_code}" and trade_date>="{start_date}" and trade_date<="{end_date}"',
            self.db_engine)
        return df

    @post_query
    def daily(self, stock_code, start_date, end_date):
        if type(stock_code) == list:
            df_all = None
            start_time = time.time()
            for i, stock in enumerate(stock_code):
                df_daily = self.__daliy_one(stock, start_date, end_date)
                if df_all is None:
                    df_all = df_daily
                else:
                    df_all = df_all.append(df_daily)
            logger.debug("获取 %s ~ %s %d 只股票的交易数据：%d 条, 耗时 %.2f 秒",
                         start_date, end_date, len(stock_code), len(df_all), time.time() - start_time)
            return df_all
        else:
            df_one = self.__daliy_one(stock_code, start_date, end_date)
            logger.debug("获取 %s ~ %s 股票[%s]的交易数据：%d 条", start_date, end_date, stock_code, len(df_one))
            return df_one

    @logging_time
    @post_query
    def daily_basic(self, stock_code, start_date, end_date):
        assert type(stock_code) == list or type(stock_code) == str, type(stock_code)
        if type(stock_code) == list:
            start_time = time.time()
            df_basics = [self.__daily_basic_one(stock, start_date, end_date) for stock in stock_code]
            logger.debug("获取%d只股票的每日基本信息数据%d条，耗时 : %.2f秒", len(stock_code), len(df_basics), time.time() - start_time)
            # print(df_basics)
            return pd.concat(df_basics)
        return self.__daily_basic_one(stock_code, start_date, end_date)

    def __daily_basic_one(self, stock_code, start_date, end_date):
        """返回每日的其他信息，主要是市值啥的"""
        df = pd.read_sql(
            f'select * from daily_basic \
                where ts_code="{stock_code}" and trade_date>="{start_date}" and trade_date<="{end_date}"',
            self.db_engine)
        return df

    # 指数日线行情
    @post_query
    def index_daily(self, index_code, start_date, end_date):
        df = pd.read_sql(
            f'select * from index_daily \
                where index_code="{index_code}" and trade_date>="{start_date}" and trade_date<="{end_date}"',
            self.db_engine)
        return df

    # 返回指数包含的股票
    @post_query
    def index_weight(self, index_code, start_date, end_date):
        # return self.tushare.index_weight(index_code, start_date)
        df = pd.read_sql(
            f'select * from index_weight \
                        where index_code="{index_code}" and trade_date>="{start_date}" and trade_date<="{end_date}"',
            self.db_engine)
        return df['con_code'].unique().tolist()

    # 获得财务数据
    @post_query
    def fina_indicator(self, stock_code, start_date, end_date):
        df = pd.read_sql(
            f'select * from fina_indicator \
                where ts_code="{stock_code}" and ann_date>="{start_date}" and ann_date<="{end_date}"', self.db_engine)
        return df

    @post_query
    def trade_cal(self, start_date, end_date, exchange='SSE'):
        return self.tushare.trade_cal(start_date, end_date, exchange)

    @post_query
    def stock_basic(self, ts_code):
        stock_codes = self.__list_to_sql_format(ts_code)
        df = pd.read_sql(f'select * from stock_basic where ts_code in ({stock_codes})', self.db_engine)
        return df

    @post_query
    def index_classify(self, level='', src='SW2014'):
        df = pd.read_sql(f'select * from index_classify where src = \'{src}\'', self.db_engine)
        return df

    @post_query
    def get_factor(self, name, stock_codes, start_date, end_date):
        stock_codes = self.__list_to_sql_format(stock_codes)
        df = pd.read_sql(f"""
            select * 
            from factor_{name} 
            where datetime>=\'{start_date}\' and 
                  datetime<=\'{end_date}\' and
                  code in ({stock_codes})
        """,
                         self.db_engine)
        return df

    def __list_to_sql_format(self, _list):
        """
        把list转成sql中in要求的格式
        ['a','b','c'] => " 'a','b','c' "
        """
        if _list != list: stock_codes = [_list]
        data = ["\'" + one + "\'" for one in _list]
        return ','.join(data)
