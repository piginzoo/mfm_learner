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
"""
import logging
import math

import tushare_utils
from example import factor_utils
import numpy as np

logger = logging.getLogger(__name__)


def load_stock_data(stock_codes, start_date, end_date):
    df_merge = None
    for stock_code in stock_codes:
        # 基本数据，包含：PE
        df_basic = tushare_utils.daily_basic(stock_code=stock_code, start_date=start_date, end_date=end_date)

        # 财务数据，包含：归母公司净利润(TTM)增长率
        df_finance = tushare_utils.fina_indicator(stock_code=stock_code, start_date=start_date, end_date=end_date)

        df_finance = df_finance.sort_values('ann_date')
        df_finance['ann_date_next'] = df_finance['ann_date'].shift(-1)
        df_basic['netprofit_yoy'] = np.NaN
        for index, finance in df_finance.iterrows():
            next_date = finance['ann_date_next']
            current_date = finance['ann_date']
            netprofit_yoy = finance['netprofit_yoy']
            # bugfix,太诡异了，如果是nan，其实nan是一个float类型的,type(nan)==<float>
            if next_date is None or (type(next_date) == float and math.isnan(next_date)):
                df_basic.loc[df_basic.trade_date > current_date,'netprofit_yoy'] = netprofit_yoy
            else:
                df_basic.loc[(df_basic.trade_date > current_date) &
                             (df_basic.trade_date <= next_date),'netprofit_yoy'] = netprofit_yoy

        if df_merge is None:
            df_merge = df_basic
        else:
            df_merge = df_merge.append(df_basic)
        logger.debug("加载%s~%s的股票[%s]的%d条PE和归母公司净利润(TTM)增长率的合并数据", start_date, end_date, stock_code, len(df_merge))
    logger.debug("一共加载%s~%s %d条数据", start_date, end_date, len(df_merge))
    return df_merge


# 计算股票的PEG值
# 输入：context(见API)；stock_list为list类型，表示股票池
# 输出：df_PEG为dataframe: index为股票代码，data为相应的PEG值
def get_factor(stock_codes, start_date, end_date):
    # 查询股票池里股票的市盈率，收益增长率
    df_stock_data = load_stock_data(stock_codes, start_date, end_date)

    # 去除PE或G值为非数字的股票所在行
    df_stock_data = df_stock_data.dropna()
    logger.debug("删除掉NAN后，剩余数据行数：%d 条", len(df_stock_data))
    assert len(df_stock_data)>0, str(len(df_stock_data))

    df_stock_data['PEG'] = df_stock_data['pe'] / df_stock_data['netprofit_yoy']
    factors = df_stock_data[['trade_date', 'ts_code', 'PEG']]

    return factor_utils.reset_index(factors)
