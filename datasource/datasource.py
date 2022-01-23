import logging
from abc import ABC, abstractmethod

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

    def wrapper(*args, **kw):
        df = func(*args, **kw)
        if type(df) != DataFrame:
            # logger.debug("不是DataFrame：%r",df)
            return df
        df = comply_field_names(df)
        return df

    return wrapper


class DataSource(ABC):

    @abstractmethod
    def daily(self, stock_code, start_date, end_date):
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
    def index_weight(self, index_code, start_date):
        pass

    # 获得财务数据
    @abstractmethod
    def fina_indicator(self, stock_code, start_date, end_date):
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

    @abstractmethod
    def fund_daily(self, fund_code, start_date, end_date):
        pass
