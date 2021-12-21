import logging
import os
import random
import time

import pandas as pd
import tushare

logger = logging.getLogger(__name__)
pro = tushare.pro_api()

BASE_DIR = "data/tushare/"
if not os.path.exists(BASE_DIR): os.makedirs(BASE_DIR)


def __random_sleep():
    """居然触发了tushare的限流，加个随机等待"""
    time.sleep(random.random())


def __get_cache(stock_code, start_date, end_date):
    file_name = "{}_{}_{}.csv".format(stock_code, start_date, end_date)
    file_path = os.path.join(BASE_DIR, file_name)
    if not os.path.exists(file_path): return
    logger.debug("使用%s~%s，股票[%s]的缓存数据", start_date, end_date, stock_code)
    return pd.read_csv(file_path)


def daily(stock_code, start_date, end_date):
    __random_sleep()
    df = __get_cache(stock_code, start_date, end_date)
    if df is not None: return df
    return pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date)


def daily_basic(stock_code, start_date, end_date):
    __random_sleep()
    df = __get_cache(stock_code, start_date, end_date)
    if df is not None: return df
    return pro.daily_basic(ts_code=stock_code, start_date=start_date, end_date=end_date)
