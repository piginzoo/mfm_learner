import functools
import logging
import os
from abc import ABC, abstractmethod

import pandas as pd
from pandas import DataFrame

from datasource.impl.fields_mapper import MAPPER
from utils import CONF

logger = logging.getLogger(__name__)


def comply_field_names(df):
    """
    按照 datasource.impl.fields_mapper.MAPPER 中定义的字段映射，对字段进行统一改名
    """
    datasource_type = CONF['datasource']
    column_mapping = MAPPER.get(datasource_type)
    if column_mapping is None: raise ValueError("字段映射无法识别映射类型(即数据类型)：" + datasource_type)
    df = df.rename(columns=column_mapping)
    return df


def post_query(func):
    """
    一个包装器，用于把数据的字段rename
    :param func:
    :return:
    """

    def wrapper_it(*args, **kw):
        df = func(*args, **kw)
        if type(df) != DataFrame:
            # logger.debug("不是DataFrame：%r",df)
            return df
        df = comply_field_names(df)
        return df

    return wrapper_it


def _get_cache_file_name(dir, func, args, kwargs):
    args = list(args) + list(kwargs.values())  # 获得所有的参数值
    args = [arg for arg in args if arg is not None] # 去掉None的参数
    file_name = "_".join(args)
    file_name = "{}_{}.csv".format(func, file_name)
    file_path = os.path.join(dir, file_name)
    return file_path


def _get_cache(dir, func, args, kwargs, str_fields=None):
    """
    :param func:
    :param stock_code:
    :param start_date:
    :param end_date:
    :param str_fields: 要转换成字符串的列名，逗号分隔
    :return:
    """
    file_path = _get_cache_file_name(dir, func, args, kwargs)
    if not os.path.exists(file_path): return None
    df = pd.read_csv(file_path)
    logger.debug("使用%s_%r缓存数据%d条", func, args, len(df))
    # 'Unnamed: 0'，是观察出来的，第一列设置成index，原始的tushare就是这样的index结构
    df = df.set_index("Unnamed: 0")

    # 由于从csv加载，很多字段被整成int，不行，得转回str
    default_str = ['trade_date', 'ann_date', 'ts_code'] # 默认要转的
    if str_fields:
        str_fields = str_fields.split(",")
        str_fields += default_str
    else:
        str_fields = default_str

    for str_field in str_fields:
        if str_field in df.columns: df[str_field] = df[str_field].astype(str)

    if len(df.columns)==1: df = df.iloc[:,0] # 如果只有一列，转成Series

    return df


def _set_cache(dir, func, df, args, kwargs):
    if not os.path.exists(dir): os.makedirs(dir)
    file_path = _get_cache_file_name(dir, func, args, kwargs)
    logger.debug("缓存%s_%r数据=>%s", func, list(args) + list(kwargs.values()), file_path)
    df.to_csv(file_path)


def cache(dir,str_fields=None):
    """
    实现了一个包装器，用来缓存数据，
    主要完成，把df自动保存pd.to_cvs()成一个csv文件，
    名字是靠参数拼接起来的，dir参数是保存的目录。
    下次再调用的时候，如果发现dir目录中存在参数拼接的文件，就直接缓存加载返回。
    :param dir:
    :return:
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 看看是不是缓存了，如果是，就直接返回缓存
            pure_args = args[1:]
            logger.debug("缓存,函数[%s],参数：%r,%r", func.__name__, pure_args, kwargs)
            df = _get_cache(dir, func.__name__, pure_args, kwargs, str_fields)
            if df is not None: return df
            # 真正的去调用函数！！！
            df = func(*args, **kwargs)
            # 调用完，把结果缓存一下
            _set_cache(dir, func.__name__, df, pure_args, kwargs)
            return df

        return wrapper

    return decorator


class DataSource():

    @abstractmethod
    def daily(self, stock_code, start_date=None, end_date=None):
        pass

    # 返回每日的其他信息，主要是市值啥的
    @abstractmethod
    def daily_basic(self, stock_code, start_date, end_date):
        pass

    # 指数日线行情
    @abstractmethod
    def index_daily(self, index_code, start_date, end_date):
        pass

    # 返回指数包含的股票
    @abstractmethod
    def index_weight(self, index_code, start_date, end_date=None):
        pass

    # 获得财务数据
    @abstractmethod
    def fina_indicator(self, stock_code, start_date, end_date):
        pass

    # 利润表
    @abstractmethod
    def income(self, stock_code, start_date, end_date):
        pass

    @abstractmethod
    def trade_cal(self, start_date, end_date, exchange='SSE'):
        pass

    @abstractmethod
    def stock_basic(self, ts_code):
        pass

    @abstractmethod
    def index_classify(self, level='', src='SW2014'):
        """行业分类"""
        pass

    # 返回基金信息
    @abstractmethod
    def fund_daily(self, fund_code, start_date, end_date):
        pass
