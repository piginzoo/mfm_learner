from datasource.impl.baostock_datasource import BaostockDataSource
from datasource.impl.database_datasource import DatabaseDataSource
from datasource.impl.tushare_datasource import TushareDataSource
from utils import CONF


def create(type=None):
    if type is None:
        type = CONF['datasource']

    if type=="tushare":
        return TushareDataSource()

    if type=="database":
        return DatabaseDataSource()

    if type=="baostock":
        return BaostockDataSource()
