from mfm_learner.datasource import datasource_utils
from mfm_learner.example.factor_utils import handle_finance_fill
from mfm_learner.example.factors.factor import Factor


class DividendRateFactor(Factor):
    """
    DividendRat
    股息率TTM=近12个月股息总额/当日总市值

    tushare样例数据：
    https://tushare.pro/document/2?doc_id=32
    - dv_ratio	float	股息率 （%）
    - dv_ttm	float	股息率（TTM）（%）
    --------------------------------------------
         ts_code     trade_date  dv_ratio dv_ttm
    0    600230.SH   20191231    3.4573  1.9361
    1    600230.SH   20191230    3.4573  1.9361
    2    600230.SH   20191227    3.4308  1.9212
    3    600230.SH   20191226    3.3629  1.8832
    4    600230.SH   20191225    3.5537  1.9900
    ..         ...        ...       ...     ...
    482  600230.SH   20180108    0.2692  0.2692
    483  600230.SH   20180105    0.2856  0.2856
    484  600230.SH   20180104    0.2805  0.2805
    485  600230.SH   20180103    0.2897  0.2897
    486  600230.SH   20180102    0.3021  0.3021
    --------------------------------------------
    诡异之处,dv_ratio是股息率，dv_ttm是股息率TTM，
    TTM应该比直接的股息率要高，对吧？
    我理解普通股息率应该是从年初到现在的分红/市值，
    而TTM还包含了去年的分红呢，理应比普通的股息率要高，
    可是，看tushare数据，恰恰是反的，困惑ing...

    TODO：目前，考虑还是直接用TTM数据了
    """

    def __init__(self):
        super().__init__()

    def name(self):
        return "dividend_rate_ttm嗯，嗯。尴尬现"

    def cname(self):
        return "股息率"

    def calculate(self, stock_codes, start_date, end_date):
        df_basic = self.datasource.daily_basic(stock_codes, start_date, end_date)
        df_basic = datasource_utils.reset_index(df_basic)
        return df_basic['dv_ttm']/100

