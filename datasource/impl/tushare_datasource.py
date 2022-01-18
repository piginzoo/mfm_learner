import datetime
import logging
import os
import random
import time

import pandas as pd
import tushare

from datasource.datasource import DataSource
from datasource.datasource_utils import post_query
import utils

logger = logging.getLogger(__name__)

BASE_DIR = "../data/tushare/"
MAX_ROWS = 1000

if not os.path.exists(BASE_DIR): os.makedirs(BASE_DIR)

"""
不小心触发了5000行的调用，被tushare限流了，所以不得不写一个类，来控制一下流量，
- 实现了一个缓存，sotck+日期作为key，重复调用不会再查，每个股票的一段时间，作为一个单独cache文件
- 实现了一个长度调用，超过MAX_ROWS=1000行，自动报警
"""


def _random_sleep():
    """居然触发了tushare的限流，加个随机等待"""
    time.sleep(random.random())


def _check_lenght(df):
    if len(df) > 1000: raise ValueError("Tushare返回结果超过1000：" + str(len(df)))


def _get_cache_file_name(func, stock_code, start_date, end_date):
    file_name = "{}_{}_{}_{}.csv".format(func, stock_code, start_date, end_date)
    file_path = os.path.join(BASE_DIR, file_name)
    return file_path


def _get_cache(func, stock_code, start_date, end_date):
    file_path = _get_cache_file_name(func, stock_code, start_date, end_date)
    if not os.path.exists(file_path): return None
    logger.debug("使用%s~%s，股票[%s]的[%s]缓存数据", start_date, end_date, stock_code, func)
    df = pd.read_csv(file_path)
    # 'Unnamed: 0'，是观察出来的，第一列设置成index，原始的tushare就是这样的index结构
    df = df.set_index("Unnamed: 0")
    if 'trade_date' in df.columns:
        # logger.debug("设置列[trade_date]为str类型")
        df.trade_date = df.trade_date.astype(str)
    if 'ann_date' in df.columns: df.ann_date = df.ann_date.astype(str)
    if 'ts_code' in df.columns:  df.ts_code = df.ts_code.astype(str)
    return df


def _set_cache(func, df, stock_code, start_date, end_date):
    file_path = _get_cache_file_name(func, stock_code, start_date, end_date)
    logger.debug("缓存%s~%s，股票[%s]的[%s]数据=>%s", start_date, end_date, stock_code, func, file_path)
    df.to_csv(file_path)


# ------------------------------------------------------------------------------------------------
# 以下是对tushare的各种API函数包装
# ------------------------------------------------------------------------------------------------

class TushareDataSource(DataSource):

    def __init__(self):
        token = utils.CONF['datasources']['tushare']['token']
        self.pro = tushare.pro_api(token)
        logger.debug("login到tushare pro上，key: %s...",token[:10])

    # 返回每日行情数据，不限字段
    # https://tushare.pro/document/2?doc_id=27
    @post_query
    def daliy_one(self, stock_code, start_date, end_date, fields=None):
        df = _get_cache('daily', stock_code, start_date, end_date)
        if df is not None: return df
        _random_sleep()
        df = self.pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date, fields=fields)
        _set_cache('daily', df, stock_code, start_date, end_date)
        _check_lenght(df)
        return df

    def trade_cal(self, start_date, end_date, exchange='SSE'):
        """
        exchange: 交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源
        """
        df = self.pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date, is_open=1)
        return df['cal_date']

    @post_query
    def daily(self, stock_code, start_date, end_date, fields=None):
        if type(stock_code) == list:
            logger.debug("获取多只股票的交易数据：%r", ",".join(stock_code))
            df_all = None
            for stock in stock_code:
                df_daily = self.daliy_one(stock, start_date, end_date, fields)
                if df_all is None:
                    df_all = df_daily
                else:
                    df_all = df_all.append(df_daily)
            logger.debug("获取 %s ~ %s 多只股票的交易数据：%d 条", start_date, end_date, len(df_all))
            return df_all
        else:
            df_one = self.daliy_one(stock_code, start_date, end_date, fields)
            logger.debug("获取 %s ~ %s 股票[%s]的交易数据：%d 条", start_date, end_date, stock_code, len(df_one))
            return df_one

    # 返回每日的其他信息，主要是市值啥的
    # https://tushare.pro/document/2?doc_id=32
    @post_query
    def daily_basic(self, stock_code, start_date, end_date, fields=None):
        df = _get_cache('daily_basic', stock_code, start_date, end_date)
        if df is not None: return df
        _random_sleep()
        df = self.pro.daily_basic(ts_code=stock_code, start_date=start_date, end_date=end_date, fields=fields)
        _set_cache('daily_basic', df, stock_code, start_date, end_date)
        _check_lenght(df)
        return df

    # 指数日线行情
    # https://tushare.pro/document/2?doc_id=95
    @post_query
    def index_daily(self, index_code, start_date, end_date, fields=None):
        df = _get_cache('index_daily', index_code, start_date, end_date)
        if df is not None: return df
        _random_sleep()
        df = self.pro.index_daily(ts_code=index_code, start_date=start_date, end_date=end_date, fields=fields)
        _set_cache('index_daily', df, index_code, start_date, end_date)
        _check_lenght(df)
        return df

    # 获得财务数据， TODO：没有按照出财务报表的时间来query
    # https://tushare.pro/document/2?doc_id=79
    @post_query
    def fina_indicator(self, stock_code, start_date, end_date, fields=None):
        df = _get_cache('fina_indicator', stock_code, start_date, end_date)
        if df is not None: return df
        _random_sleep()
        df = self.pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=end_date, fields=fields)
        _set_cache('fina_indicator', df, stock_code, start_date, end_date)
        _check_lenght(df)
        return df

    # 获得指数包含的股票，从开始日期找1年
    # https://tushare.pro/document/2?doc_id=96
    @post_query
    def index_weight(self, index_code, trade_date, fields=None):
        """
        这个返回数据量太大，每天300条，10天就300条，常常触发5000条限制，
        所以我的办法就是用start_date，去取，如果没有，就去取下个月的这个日子的，直到取得
        :param index_code:
        :param start_date:
        :return:
        """
        count = 0
        df = None
        original_trade_date = trade_date
        while count < 12:  # 尝试1年的（12个30天）

            # 看有缓存么？如果有返回
            df = _get_cache('index_weight', index_code, start_date=trade_date, end_date=trade_date)
            if df is not None:
                return df['con_code'].unique()

            _random_sleep()
            df = self.pro.index_weight(index_code=index_code, start_date=trade_date, end_date=trade_date, fields=fields)
            logger.debug("获得日期%s的指数%s的成分股：%d 个", trade_date, index_code, len(df))
            if len(df) > 0:
                break

            trade_date = datetime.datetime.strptime(trade_date, "%Y%m%d") + datetime.timedelta(days=30)
            trade_date = trade_date.strftime("%Y%m%d")
            count += 1

        assert df is not None or len(df) == 0, "取得index_weight失败：" + trade_date

        logger.debug("获得日期%s的指数%s的成分股：%d 个", trade_date, index_code, len(df))
        _check_lenght(df)
        _set_cache('index_weight', df, index_code, original_trade_date, original_trade_date)
        return df['con_code'].unique()

    # https://tushare.pro/document/2?doc_id=181
    def index_classify(self, level='L3', src='SW2014'):
        """申万行业，默认是L3:3级，2014版（还有2021版）"""
        df = _get_cache('index_classify', level, start_date=src, end_date='')
        if df is not None: return df
        _random_sleep()
        df = self.pro.index_classify(level=level,src=src)
        _set_cache('index_classify', df, index_code=level, start_date=src, end_date='')
        _check_lenght(df)
        return df


    # https://tushare.pro/document/2?doc_id=25
    def stock_basic(self, ts_code):
        df = _get_cache('stock_basic', ts_code, start_date='', end_date='')
        if df is not None: return df
        _random_sleep()
        df = self.pro.stock_basic(ts_code)
        _set_cache('stock_basic', df, ts_code, start_date='', end_date='')
        _check_lenght(df)
        return df
