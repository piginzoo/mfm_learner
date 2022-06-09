"""
#  估值因子 - PEG

参考：
- https://zhuanlan.zhihu.com/p/29144485
- https://www.joinquant.com/help/api/help#factor_values:%E6%88%90%E9%95%BF%E5%9B%A0%E5%AD%90
- https://www.joinquant.com/view/community/detail/087af3a4e27c600ed855cb0c1d0fdfed
在时间序列上，PEG因子的暴露度相对其他因子较为稳定，在近一年表现出较强的趋势性
市盈率相对盈利增长比率PEG = PE / (归母公司净利润(TTM)增长率 * 100) # 如果 PE 或 增长率为负，则为 nan
对应tushare：netprofit_yoy	float	Y	归属母公司股东的净利润同比增长率(%)

财务数据"归属母公司股东的净利润同比增长率(%)"的取得：https://tushare.pro/document/2?doc_id=79
    输入参数
        ts_code	str	Y	TS股票代码,e.g. 600001.SH/000001.SZ
        ann_date	str	N	公告日期
        start_date	str	N	报告期开始日期
        end_date	str	N	报告期结束日期
        period	str	N	报告期(每个季度最后一天的日期,比如20171231表示年报)
    输出参数
        ts_code	str	Y	TS代码
        ann_date	str	Y	公告日期
        end_date	str	Y	报告期
这里的逻辑是，某一天，只能按照公告日期来当做"真正"的日期，毕竟，比如8.30号公告6.30号之前的信息，
加入今天是7.1日，所以只能用最近的一次，也就是最后一次8.30号的，
再往前倒，比如6.15号还发布过一次，那6.15号之后到6.30号，之间的，就用6.15好的数据，
所以，报告期end_date其实没啥用，因为他是滞后的，外界是无法提前知道的。
这样处理也简单粗暴，不知道业界是怎么处理的？我感觉应该很普遍的一个问题。

            shift(-1)
current     next
            2021.1.1
---------------------
2021.1.1    2021.3.1
2021.3.1    2021.6.30
2021.6.30   2021.9.30
2021.9.30
---------------------
2021.1.1之前的，不应该用2021.1.1去填充，但是，没办法，无法获得再之前的数据，只好用他了
2021.9.30之后的，都用2021.9.30来填充

季报应该是累计数，为了可比性，所以应该做一个处理
研报上的指标用的都是TTM
PE-TTM 也称之为 滚动市盈率
TTM英文本意是Trailing Twelve Months，也就是过去12个月，非常好理解
比如当前2017年半年报刚发布完，那么过去12个月的净利润就是：
2017Q2 (2017年2季报累计值) + 2016Q4 (2016年4季度累计值) - 2016Q2 (2016年2季度累计值)
"""
import logging
import math

import numpy as np

from mfm_learner.datasource import datasource_utils
from mfm_learner.example.factors.factor import Factor

logger = logging.getLogger(__name__)


class PEGFactor(Factor):

    def __init__(self):
        super().__init__()

    def name(self):
        return "peg"

    def calculate(self, stock_codes, start_date, end_date, df_daily=None):
        """
        # 计算股票的PEG值
        # 输入：context(见API)；stock_list为list类型，表示股票池
        # 输出：df_PEG为dataframe: index为股票代码，data为相应的PEG值
        """
        df = self.load_stock_data(stock_codes, start_date, end_date)
        df['PEG'] = df['pe'] / df['netprofit_yoy']
        df = datasource_utils.reset_index(df)
        return df['PEG']

    def load_stock_data(self, stock_codes, start_date, end_date):
        df_merge = None
        for stock_code in stock_codes:
            # 基本数据，包含：PE
            df_basic = self.datasource.daily_basic(stock_code=stock_code, start_date=start_date, end_date=end_date)

            # 财务数据，包含：归母公司净利润(TTM)增长率
            df_finance = self.datasource.fina_indicator(stock_code=stock_code, start_date=start_date, end_date=end_date)

            df_finance = df_finance.sort_index(level='datetime', ascending=True)  # 从早到晚排序

            df_finance['datetime_next'] = df_finance['datetime'].shift(-1)
            df_basic['netprofit_yoy'] = np.NaN
            logger.debug("股票[%s] %s~%s 有%d条财务数据，但有%d条基础数据",
                         stock_code, start_date, end_date, len(df_finance), len(df_basic))

            for index, finance in df_finance.iterrows():

                next_date = finance['datetime_next']
                current_date = finance['datetime']
                netprofit_yoy = finance['netprofit_yoy']

                # 第一个区间，只能"2021.1.1之前的，不应该用2021.1.1去填充，但是，没办法，无法获得再之前的数据，只好用他了"
                if index == 0:
                    # logger.debug("开始 -> %s , 过滤条数 %d", current_date,
                    #              len(df_basic.loc[(df_basic.datetime <= current_date)]))
                    df_basic.loc[df_basic.datetime <= current_date, 'netprofit_yoy'] = netprofit_yoy

                # bugfix,太诡异了，如果是nan，其实nan是一个float类型的,type(nan)==<float>
                if next_date is None or (type(next_date) == float and math.isnan(next_date)):
                    df_basic.loc[df_basic.datetime > current_date, 'netprofit_yoy'] = netprofit_yoy
                    # logger.debug("%s -> 结束 , 过滤条数 %d", current_date,
                    #              len(df_basic.loc[(df_basic.datetime > current_date)]))
                else:
                    df_basic.loc[(df_basic.datetime > current_date) &
                                 (df_basic.datetime <= next_date), 'netprofit_yoy'] = netprofit_yoy
                    # logger.debug("%s -> %s , 过滤条数 %d", current_date, next_date, len(
                    #     df_basic.loc[(df_basic.datetime > current_date) & (df_basic.datetime <= next_date)]))

            if df_merge is None:
                df_merge = df_basic
            else:
                df_merge = df_merge.append(df_basic)
            # logger.debug("加载%s~%s的股票[%s]的%d条PE和归母公司净利润(TTM)增长率的合并数据", start_date, end_date, stock_code, len(df_merge))
        logger.debug("一共加载%s~%s %d条 PEG 数据", start_date, end_date, len(df_merge))

        return df_merge
