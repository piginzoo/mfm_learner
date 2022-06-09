"""
换手率因子：

- https://zhuanlan.zhihu.com/p/37232850
- https://crm.htsc.com.cn/doc/2017/10750101/6678c51c-a298-41ba-beb9-610ab793cf05.pdf  华泰~换手率类因子
- https://uqer.datayes.com/v3/community/share/5afd527db3a1a1012acad84c

换手率因子是一类很重要的情绪类因子，反映了一支股票在一段时间内的流动性强弱，和持有者平均持有时间的长短。
一般来说换手率因子的大小和股票的收益为负向关系，即换手率越高的股票预期收益越低，换手率越低的股票预期收益越高。

四个构造出来的换手率类的因子（都是与股票的日均换手率相关）：
- turnover_Nm：个股最近N个月的日均换手率，表现了个股N个月内的流动性水平。N=1,3,6
- turnover_bias_Nm：个股最近N个月的日均换手率除以个股两年内日均换手率再减去1，代表了个股N个月内流动性的乖离率。N=1,3,6
- turnover_std_Nm：个股最近N个月的日换手率的标准差，表现了个股N个月内流动性水平的波动幅度。N=1,3,6
- turnover_bias_std_Nm：个股最近N个月的日换手率的标准差除以个股两年内日换手率的标准差再减去1，代表了个股N个月内流动性的波动幅度的乖离率。N=1,3,6

这是4个因子哈，都是跟换手率相关的，他们之间具备共线性，是相关的，要用的时候，挑一个好的，或者，做因子正交化后再用。

市值中性化：换手率类因子与市值类因子存在一定程度的负相关性，我们对换手率因子首先进行市值中性化处理，从而消除了大市值对于换手率因子表现的影响。

知乎文章的结论：进行市值中性化处理之后，因子表现有明显提高。在本文的回测方法下，turnover_1m和turnover_std_1m因子表现较好。
"""

import logging

import numpy as np

from mfm_learner.datasource import datasource_utils
from mfm_learner.example.factors.factor import Factor

logger = logging.getLogger(__name__)


class TurnOverFactor(Factor):

    def __init__(self):
        super().__init__()

    def name(self):
        return ['turnover_1m',
                'turnover_3m',
                'turnover_6m',
                'turnover_2y',
                'turnover_std_1m',
                'turnover_std_3m',
                'turnover_std_6m',
                'turnover_bias_1m',
                'turnover_bias_3m',
                'turnover_bias_6m',
                'turnover_bias_std_1m',
                'turnover_bias_std_3m',
                'turnover_bias_std_6m']

    def calculate(self, stock_codes, start_date, end_date):
        df_daily_basic = self.datasource.daily_basic(stock_codes, start_date, end_date)

        """
        # https://tushare.pro/document/2?doc_id=32
                    code  datetime  turnover_rate_f       circ_mv
        0     600230.SH   20180726           4.5734  1.115326e+06
        1     600237.SH   20180726           1.7703  2.336490e+05
        """
        print(df_daily_basic)
        df_daily_basic = df_daily_basic[['datetime', 'code', 'turnover_rate_f', 'circ_mv']]
        df_daily_basic.columns = ['datetime', 'code', 'turnover_rate', 'circ_mv']

        datas = self.calculate_turnover_rate(df_daily_basic)

        logger.debug("一共加载%s~%s %d条 换手率 数据", start_date, end_date, len(datas[0]))

        return datas

    """
    nanmean:忽略nan，不参与mean，例：
        >>> a = np.array([[1, np.nan], [3, 4]])
        >>> np.nanmean(a)
        2.6666666666666665
        >>> np.nanmean(a, axis=0)
        array([2.,  4.])
        >>> np.nanmean(a, axis=1)
        array([1.,  3.5]) # may vary
    rolling:
        https://blog.csdn.net/maymay_/article/details/80241627
    # 定义因子计算逻辑
    - turnover_Nm：个股最近N个月的日均换手率，表现了个股N个月内的流动性水平。N=1,3,6
    - turnover_bias_Nm：个股最近N个月的日均换手率除以个股两年内日均换手率再减去1，代表了个股N个月内流动性的乖离率。N=1,3,6
    - turnover_std_Nm：个股最近N个月的日换手率的标准差，表现了个股N个月内流动性水平的波动幅度。N=1,3,6
    - turnover_bias_std_Nm：个股最近N个月的日换手率的标准差除以个股两年内日换手率的标准差再减去1，代表了个股N个月内流动性的波动幅度的乖离率。N=1,3,6
    """

    def calculate_turnover_rate(self, data):
        # N个月的日均换手率
        data['turnover_1m'] = data['turnover_rate'].rolling(window=20, min_periods=1).apply(func=np.nanmean)
        data['turnover_3m'] = data['turnover_rate'].rolling(window=60, min_periods=1).apply(func=np.nanmean)
        data['turnover_6m'] = data['turnover_rate'].rolling(window=120, min_periods=1).apply(func=np.nanmean)
        data['turnover_2y'] = data['turnover_rate'].rolling(window=480, min_periods=1).apply(func=np.nanmean)

        # N个月的日均换手率的标准差
        data['turnover_std_1m'] = data['turnover_rate'].rolling(window=20, min_periods=2).apply(func=np.nanstd)
        data['turnover_std_3m'] = data['turnover_rate'].rolling(window=60, min_periods=2).apply(func=np.nanstd)
        data['turnover_std_6m'] = data['turnover_rate'].rolling(window=120, min_periods=2).apply(func=np.nanstd)
        data['turnover_std_2y'] = data['turnover_rate'].rolling(window=480, min_periods=2).apply(func=np.nanstd)

        # N个月的日换手率 / 两年内日换手率 - 1，表示N个月流动性的乖离率
        data['turnover_bias_1m'] = data['turnover_1m'] / data['turnover_2y'] - 1
        data['turnover_bias_3m'] = data['turnover_3m'] / data['turnover_2y'] - 1
        data['turnover_bias_6m'] = data['turnover_6m'] / data['turnover_2y'] - 1

        # N个月的日换手率的标准差 / 两年内日换手率的标准差 - 1，表示N个月波动幅度的乖离率
        data['turnover_bias_std_1m'] = data['turnover_std_1m'] / data['turnover_std_2y'] - 1
        data['turnover_bias_std_3m'] = data['turnover_std_3m'] / data['turnover_std_2y'] - 1
        data['turnover_bias_std_6m'] = data['turnover_std_6m'] / data['turnover_std_2y'] - 1

        data = datasource_utils.reset_index(data)

        return \
            data['turnover_1m'], \
            data['turnover_3m'], \
            data['turnover_6m'], \
            data['turnover_2y'], \
            data['turnover_std_1m'], \
            data['turnover_std_3m'], \
            data['turnover_std_6m'], \
            data['turnover_bias_1m'], \
            data['turnover_bias_3m'], \
            data['turnover_bias_6m'], \
            data['turnover_bias_std_1m'], \
            data['turnover_bias_std_3m'], \
            data['turnover_bias_std_6m']
