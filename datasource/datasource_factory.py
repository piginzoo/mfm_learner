import logging

from datasource.impl.akshare_datasource import AKShareDataSource
from datasource.impl.baostock_datasource import BaostockDataSource
from datasource.impl.database_datasource import DatabaseDataSource
from datasource.impl.tushare_datasource import TushareDataSource
from utils import CONF

logger = logging.getLogger(__name__)

__tushare_datasource = None
__akshare_datasource = None
__database_datasource = None
__baostock_datasource = None


def get():
    return create(CONF['datasource'])


def create(type=None):
    if type is None:
        type = CONF['datasource']

    logger.info("使用数据源：%s", type)

    if type == "tushare":
        global __tushare_datasource
        if not __tushare_datasource:
            __tushare_datasource = TushareDataSource()
        return __tushare_datasource

    if type == "database":
        global __database_datasource
        if not __database_datasource:
            __database_datasource = DatabaseDataSource()
        return __database_datasource

    if type == "baostock":
        global __baostock_datasource
        if not __baostock_datasource:
            __baostock_datasource = BaostockDataSource()
        return __baostock_datasource

    if type == "akshare":
        global __akshare_datasource
        if not __akshare_datasource:
            __akshare_datasource = AKShareDataSource()
        return __akshare_datasource

    raise ValueError("无效的数据源：" + type)
