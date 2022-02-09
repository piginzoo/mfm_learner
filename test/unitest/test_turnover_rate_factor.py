from utils import utils
utils.init_logger()
from datasource import datasource_factory
from example.factors.turnover_rate import TurnOverFactor


# pytest test/unitest/test_turnover_rate_factor.py -s
def test_ivff():
    turnover_factor = TurnOverFactor()

    start_date = '20200101'
    end_date = '20200130'
    index_code = "000905.SH"
    stocks = datasource_factory.get().index_weight(index_code, start_date)
    stocks = stocks[:5]
    df = turnover_factor.calculate(stocks, start_date, end_date)
    df = df.reset_index()
    df = df.set_index(['datetime','code'])
    df = df.unstack('code')
    print(df)
