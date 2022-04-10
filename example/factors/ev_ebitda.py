
'''
企业价值倍数EV/EBITDA
- https://nai500.com/zh-hans/blog/2021/04/%E4%BC%B0%E5%80%BC%E6%B3%95-%E4%BC%81%E4%B8%9A%E4%BB%B7%E5%80%BC%E5%80%8D%E6%95%B0-ev-ebitda/
- https://zhuanlan.zhihu.com/p/34793759
'''
from datasource import datasource_utils
from example import factor_utils
from example.factors.factor import Factor
from utils import utils


class EVEBITDAFactor(Factor):
    """
    """

    def __init__(self):
        super().__init__()

    def name(self):
        return "ebitda_ttm"

    def calculate(self, stock_codes, start_date, end_date):
        start_date_1years_ago = utils.last_year(start_date, num=1)
        trade_dates = self.datasource.trade_cal(start_date, end_date)
        df_finance = self.datasource.income(stock_codes, start_date_1years_ago, end_date)

        # TODO 懒得重新下载fina_indicator，临时trick一下
        df_finance['end_date'] = df_finance['end_date'].apply(str)

        assert len(df_finance) > 0
        df = factor_utils.handle_finance_ttm(stock_codes,
                                             df_finance,
                                             trade_dates,
                                             col_name_value='ebitda',
                                             col_name_finance_date='end_date')

        df = datasource_utils.reset_index(df)
        return df['ebita_ttm']



