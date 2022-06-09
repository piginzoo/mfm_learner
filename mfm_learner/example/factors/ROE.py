import logging

from mfm_learner.datasource import datasource_utils
from mfm_learner.example import factor_utils
from mfm_learner.example.factors.factor import Factor
from mfm_learner.utils import utils

logger = logging.getLogger(__name__)


class ROETTMFactor(Factor):
    """

    ROE计算：https://baike.baidu.com/item/%E5%87%80%E8%B5%84%E4%BA%A7%E6%94%B6%E7%9B%8A%E7%8E%87
    定义公式：
    1、净资产收益率=净利润*2/（本年期初净资产+本年期末净资产）
    2、杜邦公式（常用）
        - 净资产收益率=销售净利率*资产周转率*杠杆比率
        - 净资产收益率 =净利润 /净资产
        -净资产收益率= （净利润 / 销售收入）*（销售收入/ 总资产）*（总资产/净资产）

    ROE是和分期相关的，所以年报中的ROE的分期是不同的，有按年、季度、半年等分的，所以无法统一比较，
    解决办法，是都画为TTM，即从当日开始，向前回溯一整年，统一成期间为1整年的ROE_TTM(Trailing Twelve Month)
    但是，由于财报的发布滞后，所以计算TTM的时候，需要考虑这种滞后性，后面会详细讨论：
    ------------------------------------------------------------------------
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
    ------------------------------------------------------------------------
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
    ------------------------------------------------------------------------
    这个确实是问题，我梳理一下问题，虽然解决不了，但是至少要自己门清：
    - 多只股票的ROE_TTM都会滞后，最多可能会之后3-4个月（比如4.30号才得到去年的年报）
    - 多只股票可能都无法对齐ROE_TTM，比如上例中A用的是当期的，而极端的D，用的居然是截止去年10.30号发布的9.30号的3季报的数据了
    """

    def __init__(self):
        super().__init__()

    def name(self):
        return "roe_ttm"

    def calculate(self, stock_codes, start_date, end_date):

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


class ROEYOYFactor(Factor):
    """
    ROEYoY：ROE Year over Year，同比增长率
    关于ROE计算：https://baike.baidu.com/item/%E5%87%80%E8%B5%84%E4%BA%A7%E6%94%B6%E7%9B%8A%E7%8E%87

    遇到一个问题：
    SELECT ann_date,end_date,roe_yoy
    FROM tushare.fina_indicator
    where ts_code='600000.SH' and ann_date='20180428'
    ------------------------------------------------------
    '20180428','20180331','-12.2098'
    '20180428','20171231','-11.6185'
    ------------------------------------------------------
    可以看出来，用code+datetime作为索引，可以得到2个roe_yoy，这是因为，一个是年报和去年同比，一个是季报和去年同比，
    而且，都是今天发布的，所以，这里的我们要理解，yoy，同比，比的可能是同季度的，也可能是半年的，一年的，
    如果严格的话，应该使用roe_ttm，然后去和去年的roe_ttm比较，这样最准，但是，这样处理就太复杂了，
    所以，折中的还是用甲股票和乙股票，他们自己的当日年报中提供的yoy同比对比，比较吧。
    这种折中，可能潜在一个问题，就是我可能用我的"季报同比"，对比了，你的"年报同比"，原因是我们同一日、相近日发布的不同scope的财报，
    这是一个问题，最好的解决办法，还是我用我的ROE_TTM和我去年今日的ROE_TTM，做同比；然后，再和你的结果比，就是上面说的方法。
    算了，还是用这个这种方法吧，
    所以，上述的问题，是我自己（甲股票）同一天发了2份财报（年报和季报），这个时候我取哪个yoy同比结果呢，
    我的解决之道是，随便，哪个都行

    """

    def __init__(self):
        super().__init__()

    def name(self):
        return "roe_yoy"

    def cname(self):
        return "净资产收益率同比增长率(ROE变动)"

    def calculate(self, stock_codes, start_date, end_date):

        df = factor_utils.handle_finance_fill(self.datasource,stock_codes,start_date,end_date,'roe_yoy')

        assert len(df) > 0
        df = datasource_utils.reset_index(df)

        if not df.index.is_unique:
            old_len = len(df)
            df = df[~df.index.duplicated()]
            logger.warning("因子主键[日期+股票]重复，合计抛弃%d条", old_len - len(df))

        assert df.columns == ['roe_yoy']

        return df
