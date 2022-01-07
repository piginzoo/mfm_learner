import logging

from datasource.datasource import DataSource

logger = logging.getLogger(__name__)


class BaostockDataSource(DataSource):
    """
    还没实现，抽空实现一下
    """

    def daily(self, stock_code, start_date, end_date):
        pass

    # 返回每日的其他信息，主要是市值啥的
    def daily_basic(self, stock_code, start_date, end_date):
        pass

    # 指数日线行情
    def index_daily(self, index_code, start_date, end_date):
        pass

    # 返回指数包含的股票
    def index_weight(self, index_code, start_date):
        pass

    # 获得财务数据
    def fina_indicator(self, stock_code, start_date, end_date):
        pass

    def trade_cal(self, start_date, end_date, exchange='SSE'):
        pass
