# pytest  test/unitest/test_multifactors_strategy.py -s

from pandas import DataFrame

from mfm_learner.utils import utils

utils.init_logger()
from mfm_learner.datasource import datasource_utils
from mfm_learner.example.backtest.strategy_multifactors import MultiFactorStrategy
import backtrader as bt


def test_neutralize():
    """
    测试行业中性化
    def neutralize(factor_df,
                   group,
                   float_mv=None,
                   index_member=None):
    """
    date = utils.str2date('20200101')
    factors = __generate_mock_factors(date)

    select_stocks = MultiFactorStrategy.select_stocks_by_score(None,factors, date)
    assert select_stocks.tolist() == ['600001.SH']


def __generate_mock_factors(date):
    """
    因子造假器，哈哈哈
    :return:
    """

    stocks = ['600001.SH', '600002.SH', '600003.SH', '600004.SH', '600005.SH']
    factor_names = ['factor1', 'factor2', 'factor3']
    factor_dict = {}

    for factor_name in factor_names:
        df = DataFrame()
        for s in stocks:
            df = df.append([[date, s, 0.1]])

        df.columns = ['datetime', 'code', 'value']
        df.loc[df['code'] == '600001.SH', 'value'] = 0.2  # 600001.SH 股票的因子值大一些
        df = datasource_utils.reset_index(df)
        print("因子：", factor_name)
        print(df)

        factor_dict[factor_name] = df

    return factor_dict
