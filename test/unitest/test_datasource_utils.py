# pytest  test/unitest/test_datasource_utils.py -s
import utils
import pandas as pd

from datasource.impl.akshare_datasource import AKShareDataSource

utils.utils.init_logger()
from datasource import datasource_utils as dsu
from datasource.impl.tushare_datasource import TushareDataSource


def test_compile_industry():
    """
            index_code industry_name level industry_code is_pub parent_code
    10   801110.SI          家用电器    L1        330000   None           0
    22   801750.SI           计算机    L1        710000   None           0
    30   801022.SI          其他采掘    L2        210300   None      210000
    """
    data = pd.Series(["家用电器", "其他采掘", "计算机设备"])
    data = dsu.compile_industry(data)
    assert data[0] == "330000"
    assert data[1] == "210000"
    assert data[2] == "710000"

def test_tushare_cache():
    ts = TushareDataSource()
    df = ts.fina_indicator('600000.SH', start_date='20170101', end_date='20180801')
    print(df)
    df = ts.fina_indicator('600000.SH', start_date='20170101', end_date='20180801')
    print(df)

    df = ts.index_daily('600000.SH', start_date='20170101', end_date='20180801')
    print(df)
    df = ts.index_daily('600000.SH', start_date='20170101', end_date='20180801')
    print(df)

    df = ts.daliy_one('600000.SH', start_date='20170101', end_date='20180801')
    print(df)
    df = ts.daliy_one('600000.SH', start_date='20170101', end_date='20180801')
    print(df)

    df = ts.daily_basic('600000.SH', start_date='20170101', end_date='20180801')
    print(df)
    df = ts.daily_basic('600000.SH', start_date='20170101', end_date='20180801')
    print(df)

    df = ts.index_classify(level='', src='SW2014')
    print(df)
    df = ts.index_classify(level='', src='SW2014')
    print(df)


    df = ts.stock_basic('600000.SH')
    print(df)
    df = ts.stock_basic('600000.SH')
    print(df)

def test_akshare_cache():
    ak = AKShareDataSource()
    df = ak.fund_daily('710001',start_date=None, end_date=None)
    print(df)