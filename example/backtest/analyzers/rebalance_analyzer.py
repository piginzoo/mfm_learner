import logging
from collections import namedtuple

import backtrader as bt
from pandas import DataFrame

from utils import utils

logger = logging.getLogger(__name__)

class RebalanceAnalyzer(bt.Analyzer):
    """
    尝试用指标，发现很不好用，指标的那些破函数类，比如PctChange，
    所以放弃了，就直接粗暴得到line=>list=>dataframe，然后用dataframe来算了。
    """

    def create_analysis(self):
        self.rets = bt.AutoOrderedDict()

    def stop(self):
        self.rets.rebalance_rate = sum(self.strategy.rebalance_rates)/len(self.strategy.rebalance_rates)