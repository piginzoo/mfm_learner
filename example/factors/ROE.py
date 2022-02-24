import logging

from datasource import datasource_utils
from example import factor_utils
from example.factors.factor import Factor
from utils import utils

logger = logging.getLogger(__name__)


class ROEFactor(Factor):
    """
          ts_code  ann_date  end_date      roe
    0   600000.SH  20211030  20210930   6.3866
    1   600000.SH  20211030  20210930   6.3866
    2   600000.SH  20210828  20210630   4.6233
    3   600000.SH  20210430  20210331   2.8901
    4   600000.SH  20210430  20210331   2.8901
    5   600000.SH  20210327  20201231   9.7856
    6   600000.SH  20201031  20200930   7.9413

    ROE是净资产收益率，ann_date是真实发布日，end_date是对应财报上的统计截止日，
    所以，指标有滞后性，所以，以ann_date真实发布日为最合适，
    为了可比性，要做ttm才可以，即向前滚动12个月，
    但是，即使TTM了，还要考虑不同股票的对齐时间，
    比如截面时间是4.15日：
    A股票：3.31号发布1季报，他的TTM，就是 roe_1季报 + roe_去年年报 - roe_去年1季报
    B股票：4.10号发布1季报，他的TTM，就是 roe_1季报 + roe_去年年报 - roe_去年1季报
    C股票：还没有发布1季报，但是他在3.31号发布了去年年报，所以，他只能用去年的年报数据了
    D股票：还没有发布1季报，也没发布去年年报，所以，他只能用去年的3季报（去年10.31日最晚发布）
    ---------
    因子的值，需要每天都要计算出来一个值，即每天都要有一个ROE_TTM，
    所以，每天的ROE_TTM，一定是回溯到第一个可以找到的财报发表日，然后用那个发布日子，计算那之前的TTM，
    举个例子，
    我计算C股票的4.15日的ROE_TTM，就回溯到他是3.31号发布的财报，里面披露的是去年的年报的ROE
    我计算B股票的4.15日的ROE_TTM，就回溯到他是4.10号发布的财报，里面披露的是今年1季报ROE+去年的年报ROE-去年1季报的ROE
    这里有个小的问题，或者说关键点：
    就是A股票和B股票，实际上比的不是同一个时间的东东，
    A股票是去年的12个月TTM的ROE，
    B股票则是去年1季度~今年1季度12个月的TTM，
    他俩没有完全对齐，差了1个季度，
    我自己觉得，可能这是"最优"的比较了把，毕竟，我是不能用"未来"数据的，
    我站在4.15日，能看到的A股票的信息，就是去年他的ROE_TTM，虽然滞后了1个季度，但是总比没有强。
    1个季度的之后，忍了。
    ---------
    这个确实是问题，我梳理一下问题，虽然解决不了，但是至少要自己门清：
    - 多只股票的ROE_TTM都会滞后，最多可能会之后3-4个月（比如4.30号才得到去年的年报）
    - 多只股票可能都无法对齐ROE_TTM，比如上例中A用的是当期的，而极端的D，用的居然是截止去年10.30号发布的9.30号的3季报的数据了
    """

    def __init__(self):
        super().__init__()

    def name(self):
        return "roe_ttm"

    def calculate(self, stock_codes, start_date, end_date):
        """
        # 计算股票的PEG值
        # 输入：context(见API)；stock_list为list类型，表示股票池
        # 输出：df_PEG为dataframe: index为股票代码，data为相应的PEG值
        """

        start_date_2years_ago = utils.last_year(start_date, num=2)
        trade_dates = self.datasource.trade_cal(start_date, end_date)
        df_finance = self.datasource.fina_indicator(stock_codes, start_date_2years_ago, end_date)

        # TODO 懒得重新下载fina_indicator，临时trick一下
        df_finance['end_date'] = df_finance['end_date'].apply(str)

        assert len(df_finance) > 0
        df = factor_utils.handle_finance_ttm(stock_codes,
                                             df_finance,
                                             trade_dates,
                                             col_name_value='roe',
                                             col_name_finance_date='end_date')

        df = datasource_utils.reset_index(df)
        return df['roe_ttm']
