from datasource.impl.baostock_datasource import BaostockDataSource
from datasource.impl.database_datasource import DatabaseDataSource
from datasource.impl.tushare_datasource import TushareDataSource


def create(type):
    if type=="tushare":
        return TushareDataSource()

    if type=="database":
        return DatabaseDataSource()

    if type=="baostock":
        return BaostockDataSource()
