from mfm_learner.datasource import datasource_utils
from mfm_learner.example.factor_utils import handle_finance_fill
from mfm_learner.example.factors.factor import Factor


class AssetsDebtRateFactor(Factor):

    def __init__(self):
        super().__init__()

    def name(self):
        return "assets_debt_rate"

    def cname(self):
        return "资产负债率"

    def calculate(self, stock_codes, start_date, end_date):
        df = handle_finance_fill(self.datasource,
                                 stock_codes,
                                 start_date,
                                 end_date,
                                 finance_index_col_name_value='debt_to_assets')

        df = datasource_utils.reset_index(df)
        return df['debt_to_assets'] / 100  # 百分比是0~100，要变成小数
