# coding: utf-8

# 第一个因子:
# clv: close location value,
# ( (close-day_low) - (day_high - close) ) / (day_high - day_low)
# 这玩意，是一个股票的，每天都有，是一个数，
# 我们要从一堆的股票N只中，得到N个这个值，可以形成一个截面，
# 用这个截面，我们可以拟合出β和α，
# 然后经过T个周期（交易日），就可以有个T个β和α，
# 因子长这个样子：
# trade_date
#  	      	000001.XSHE  000002.XSHE
# 2019-01-02  -0.768924    0.094851     。。。。。
# 。。。。。。
# 类比gpd、股指（市场收益率），这个是因子不是一个啊？而是多个股票N个啊？咋办？
#
# 参考：https://www.bilibili.com/read/cv13893224?spm_id_from=333.999.0.0
import logging

import pandas as pd

from utils import tushare_dbutils, factor_utils

logger = logging.getLogger(__name__)


def get_factor(stock_codes, start_date, end_date):
    df_merge = pd.DataFrame()
    # 每支股票
    for stock_code in stock_codes:
        # 得到日交易数据
        data = tushare_dbutils.daily(stock_code=stock_code, start_date=start_date, end_date=end_date)
        # data.info()
        data = data.sort_values(['trade_date'])
        # 计算CLV因子
        data['CLV'] = ((data['close'] - data['low']) - (data['high'] - data['close'])) / (data['high'] - data['low'])
        # 处理出现一字涨跌停
        data.loc[(data['high'] == data['low']) & (data['open'] > data['pre_close']), 'CLV'] = 1
        data.loc[(data['high'] == data['low']) & (data['open'] < data['pre_close']), 'CLV'] = -1

        # logger.debug("加载%s~%s的股票[%s]的 %d 条CLV数据", start_date, end_date, stock_code, len(data))
        if df_merge is None:
            df_merge = data
        else:
            df_merge = df_merge.append(data)
    logger.debug("一共加载%s~%s %d条 CLV 数据", start_date, end_date, len(df_merge))

    factors = df_merge[['trade_date', 'ts_code', 'CLV']]

    return factor_utils.reset_index(factors)
