import logging
from abc import ABC, abstractmethod

from mfm_learner.datasource import datasource_factory
from mfm_learner.utils import CONF
import pandas as pd
from sqlalchemy import Table, MetaData

from mfm_learner.utils import utils

logger = logging.getLogger(__name__)


class Factor(ABC):
    def __init__(self):
        self.datasource = datasource_factory.create(CONF['datasource'])

    # 英文名
    @abstractmethod
    def name(self):
        pass

    # 中文名
    def cname(self):
        return "未定义"

    @abstractmethod
    def calculate(self, stock_codes, start_date, end_date, df_daily=None, df_basic=None):
        """
        如果不提供df_daily、df_basic，因子需要自己去通过self.datasource，自己去获得相关的数据。

        :param stock_codes: 本因子涉及到的股票
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param df_daily: 每天的交易信息（交易数据）
        :param df_basic: 每天的基本信息（市值等）
        :return: 因子值
        """
        pass

