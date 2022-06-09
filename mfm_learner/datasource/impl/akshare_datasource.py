import logging
from datetime import datetime

import akshare as ak

from mfm_learner.datasource.datasource import DataSource, post_query, cache

logger = logging.getLogger(__name__)


class AKShareDataSource(DataSource):

    def __date_format(self, s_date: str):
        """
        YYYYMMDD=>YYYY-MM-DD
        """
        assert len(s_date) == 8  # 格式YYYYMMDD
        return datetime.strftime(datetime.strptime(s_date, "%Y%m%d"), "%Y-%m-%d")


    @post_query
    def daily(self, stock_code, start_date, end_date):
        pass

    # 返回每日的其他信息，主要是市值啥的
    @post_query
    def daily_basic(self, stock_code, start_date, end_date):
        pass

    # 指数日线行情
    @post_query
    def index_daily(self, index_code, start_date, end_date):
        pass

    # 返回指数包含的股票
    @post_query
    def index_weight(self, index_code, start_date):
        pass

    # 获得财务数据
    @post_query
    def fina_indicator(self, stock_code, start_date, end_date):
        pass

    @post_query
    def trade_cal(self, start_date, end_date, exchange='SSE'):
        pass

    # 返回基金信息: https://www.akshare.xyz/data/fund/fund_public.html?highlight=%E5%9F%BA%E9%87%91
    @post_query
    @cache("./data/akshare")
    def fund_daily(self, code, start_date, end_date):
        df = ak.fund_em_open_fund_info(fund=code, indicator="累计净值走势")
        df.columns = ["datetime","close"]
        df['open'] = df['close']
        return df
