import datetime
import logging
import random
import time

import tushare

import utils
from datasource.datasource import DataSource, post_query, cache

logger = logging.getLogger(__name__)

BASE_DIR = "./data/tushare/"
MAX_ROWS = 1000

"""
不小心触发了5000行的调用，被tushare限流了，所以不得不写一个类，来控制一下流量，
- 实现了一个缓存，sotck+日期作为key，重复调用不会再查，每个股票的一段时间，作为一个单独cache文件
- 实现了一个长度调用，超过MAX_ROWS=1000行，自动报警
"""


def _random_sleep():
    """居然触发了tushare的限流，加个随机等待"""
    time.sleep(random.random())


def _check_lenght(df):
    if len(df) > 4000: raise ValueError("为防止被封，Tushare返回结果不超过4000：" + str(len(df)))


# ------------------------------------------------------------------------------------------------
# 以下是对tushare的各种API函数包装
# ------------------------------------------------------------------------------------------------

class TushareDataSource(DataSource):

    def __init__(self):
        token = utils.CONF['datasources']['tushare']['token']
        self.pro = tushare.pro_api(token)
        logger.debug("login到tushare pro上，key: %s...", token[:10])

    # 返回每日行情数据，不限字段
    # https://tushare.pro/document/2?doc_id=27
    @post_query
    @cache(BASE_DIR)
    def daliy_one(self, stock_code, start_date, end_date, fields=None):
        _random_sleep()
        df = self.pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date, fields=fields)
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
    @cache(BASE_DIR)
    def daily_basic(self, stock_code, start_date, end_date, fields=None):
        _random_sleep()
        if type(stock_code) == list: stock_code = ",".join(stock_code)
        df = self.pro.daily_basic(ts_code=stock_code, start_date=start_date, end_date=end_date, fields=fields)
        _check_lenght(df)
        return df

    # 指数日线行情
    # https://tushare.pro/document/2?doc_id=95
    @post_query
    @cache(BASE_DIR)
    def index_daily(self, index_code, start_date, end_date, fields=None):
        _random_sleep()
        df = self.pro.index_daily(ts_code=index_code, start_date=start_date, end_date=end_date, fields=fields)
        return df

    # https://tushare.pro/document/2?doc_id=79
    @post_query
    @cache(BASE_DIR)
    def fina_indicator(self, stock_code, start_date, end_date, fields=None):
        _random_sleep()
        df = self.pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=end_date, fields=fields)
        _check_lenght(df)
        return df

    # 获得财务数据， TODO：没有按照出财务报表的时间来query

    # 获得指数包含的股票，从开始日期找1年
    # https://tushare.pro/document/2?doc_id=96
    @post_query
    @cache(BASE_DIR)
    def index_weight(self, index_code, start_date, end_date=None):
        """
        这个返回数据量太大，每天300条，10天就300条，常常触发5000条限制，
        所以我的办法就是用start_date，去取，如果没有，就去取下个月的这个日子的，直到取得
        :param index_code:
        :param start_date:
        :return:
        """
        count = 0
        df = None
        while count < 12:  # 尝试1年的（12个30天）
            _random_sleep()
            df = self.pro.index_weight(index_code=index_code, start_date=start_date, end_date=start_date)
            logger.debug("获得日期%s的指数%s的成分股：%d 个", start_date, index_code, len(df))
            if len(df) > 0:
                break
            start_date = datetime.datetime.strptime(start_date, "%Y%m%d") + datetime.timedelta(days=30)
            start_date = start_date.strftime("%Y%m%d")
            count += 1

        assert df is not None or len(df) == 0, "取得index_weight失败：" + start_date
        logger.debug("获得日期%s的指数%s的成分股：%d 个", start_date, index_code, len(df))
        _check_lenght(df)
        return df['con_code'].uniq().tolist()

    # https://tushare.pro/document/2?doc_id=181
    @post_query
    @cache(BASE_DIR,'parent_code,index_code,industry_code')
    def index_classify(self, level='', src='SW2014'):
        # """申万行业，2014版（还有2021版）"""
        _random_sleep()
        df = self.pro.index_classify(level=level, src=src)
        _check_lenght(df)
        return df

    # https://tushare.pro/document/2?doc_id=25
    @post_query
    @cache(BASE_DIR)
    def stock_basic(self, ts_code):
        """股票基本信息，主要是为了获得行业信息（目前）"""
        _random_sleep()
        df = self.pro.stock_basic(ts_code=ts_code)
        _check_lenght(df)
        return df

    # https://tushare.pro/document/2?doc_id=119
    @post_query
    def fund_daily(self, fund_code, start_date, end_date):
        df = self.pro.fund_daily(ts_code=fund_code, start_date=start_date, end_date=end_date)
        return df
