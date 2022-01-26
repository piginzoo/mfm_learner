from utils import utils
utils.init_logger()
from datasource import datasource_factory
from example.factors.ivff import IVFFFactor


# pytest test/unitest/test_ivff.py -s
def test_ivff():
    ivff = IVFFFactor()

    start_date = '20200101'
    end_date = '20200130'
    index_code = "000905.SH"
    stocks = datasource_factory.get().index_weight(index_code, start_date)
    stocks = stocks.iloc[:,0].unique()
    stocks = stocks[:5].tolist()
    df = ivff.calculate(stocks, start_date, end_date)
    df = df.reset_index()
    df = df.set_index(['datetime','code'])
    df = df.unstack('code')
    print(df)
