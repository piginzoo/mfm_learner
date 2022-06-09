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

from mfm_learner.datasource import datasource_utils
from mfm_learner.example.factors.factor import Factor

logger = logging.getLogger(__name__)


class CLVFactor(Factor):

    def __init__(self):
        super().__init__()

    def name(self):
        return "clv"

    def calculate(self, stock_codes, start_date, end_date, df_daily=None):
        if df_daily is None:
            df_daily = datasource_utils.load_daily_data(self.datasource, stock_codes, start_date, end_date)

        # 计算CLV因子
        df_daily['CLV'] = ((df_daily['close'] - df_daily['low']) - (df_daily['high'] - df_daily['close'])) / (
                df_daily['high'] - df_daily['low'])
        # 处理出现一字涨跌停
        df_daily.loc[(df_daily['high'] == df_daily['low']) & (df_daily['open'] > df_daily['pre_close']), 'CLV'] = 1
        df_daily.loc[(df_daily['high'] == df_daily['low']) & (df_daily['open'] < df_daily['pre_close']), 'CLV'] = -1

        df_daily = datasource_utils.reset_index(df_daily)
        factors = df_daily['CLV']
        logger.debug("一共加载%s~%s %d条 CLV 数据", start_date, end_date, len(factors))

        return factors
