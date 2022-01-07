import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class DataSource(ABC):

    @abstractmethod
    def daily(self, stock_code, start_date, end_date):
        pass

    # 返回每日的其他信息，主要是市值啥的
    @abstractmethod
    def daily_basic(self, stock_code, start_date, end_date):
        pass

    # 指数日线行情
    @abstractmethod
    def index_daily(self, index_code, start_date, end_date):
        pass

    # 返回指数包含的股票
    @abstractmethod
    def index_weight(self, index_code, start_date):
        pass

    # 获得财务数据
    @abstractmethod
    def fina_indicator(self, stock_code, start_date, end_date):
        pass

    @abstractmethod
    def trade_cal(self, start_date, end_date, exchange='SSE'):
        pass
