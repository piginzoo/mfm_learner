import logging

import numpy as np
import pandas as pd
from backtrader import Trade

from mfm_learner.example.backtest.trade_listener import TradeListener
from mfm_learner.utils import utils

logger = logging.getLogger(__name__)


class RiskControl(TradeListener):
    """
    实现多因子的风控策略，
    目前实现了两种：
    1、是单个持仓期间的股票，他是否下跌达到了N*ATR，就清仓
    2、整体价值（市值+现金）下跌了2*STD，后者是15%，就清仓
    """

    def __init__(self, strategy, atr_times, period):
        self.strategy = strategy
        self.period = period
        self.atr_times = atr_times
        self.current_stocks_highest_price = {}  # 保存当前的最高价格的股票+其价格
        self.portfolio_highest_value = self.strategy.broker.getvalue()  # 组合的最高价格
        self.portfolio_values = []  # 组合每天的市值
        logger.debug("创建风控对象：组合当前最高市值[%.2f]", self.portfolio_highest_value)

    def _volatility(self):
        return pd.Series(self.portfolio_values).pct_change().std() * 3

    def portfolio_risk_control(self):
        """
        做组合总资产的风控
        :return:  True，整体清仓  False，无动作
        """

        # 使用当天的开盘价，来计算风险，也就是，如果开市就出现异常，就需要卖出
        today_value = self.strategy.broker.getvalue()

        # 如果今天市值创新高，那么记录他即可
        if today_value > self.portfolio_highest_value:
            self.portfolio_highest_value = today_value
            return False

        if np.isnan(self._volatility()): return False

        if len(self.portfolio_values) < 2 * self.period: return False

        # 计算回撤
        drawback = self.portfolio_highest_value - today_value

        if drawback < self._volatility():
            # 如果回撤小于2*波动率，不理睬
            logger.debug("经过[%d]个交易周期(大于%d周期)后，总体出现回撤[%.2f] < 2*波动率(标准差[%.2f])，无视总体风险",
                         len(self.portfolio_values),
                         2 * self.period,
                         drawback,
                         self._volatility())
            return False

        # 如果回撤大于3倍STD，就要整体清仓了
        for stock_code in self.strategy.current_stocks:
            self.strategy.sell_out(stock_code)

        logger.debug("回撤[%.2f] > 3*波动率(标准差[%.2f])，全部清仓%d只股票",
                     drawback,
                     self._volatility(),
                     len(self.strategy.current_stocks))
        # 彻底退出回测
        self.strategy.env.runstop()
        self.strategy.stop_flag = True

        logger.debug("$$$$$$$$$$$$$$$$$$$$$$全部清仓，退出整个回测交易$$$$$$$$$$$$$$$$$$$$$$")
        return True

    def execute(self):
        """
        对持仓进行分线控制：
        风控规则：每天更新每支股票的历史最高价，如果当日回撤大于N倍ATR，则触发风控（清仓）
        返回：触发风控清仓的股票列表。
        """
        self.portfolio_values.append(self.strategy.broker.getvalue())

        # 如果True就代表整体清仓，就不在做个股的风控了
        if self.portfolio_risk_control(): return

        exclude_stock_codes = []

        current_date = self.strategy.datas[0].datetime.datetime(0)

        # 处理每一只股票，都要保存期历史最高价
        for d in self.strategy.datas:
            stock_code = d._name

            # 如果股票不在列表内，不做任何动作
            if stock_code not in self.strategy.current_stocks: continue

            open_price = d.open[0]
            atr = d.atr[0]
            highest_price = self.current_stocks_highest_price.get(stock_code)
            if open_price > highest_price:
                self.current_stocks_highest_price[stock_code] = open_price
                continue

            # import pdb;pdb.set_trace()

            # 如果股票在持仓列表，且满足最大回撤达到N倍ATR，则触发风控
            logger.debug("日期[%s] 股票[%s] 历史最高价[%.2f] 当前价[%.2f] 回撤[%.2f] ATR[%.2f]",
                         utils.date2str(current_date),
                         stock_code,
                         open_price,
                         highest_price,
                         highest_price - open_price,
                         self.atr_times * atr)

            drawback = highest_price - open_price
            if drawback > self.atr_times * atr:
                logger.warning("日期[%s] 股票[%s]的开盘价[%.2f]的回撤[%.2f]大于%d倍的ATR[%.2f]，触发风控清仓",
                               utils.date2str(current_date),
                               stock_code,
                               open_price,
                               drawback,
                               self.atr_times,
                               atr)
                self.strategy.sell_out(stock_code)
                exclude_stock_codes.append(stock_code)
        return exclude_stock_codes

    def on_trade(self, trade):
        stock_code = trade.data._name

        # 新创建交易，那么就是认为是买入
        if trade.status == Trade.Open:
            self.current_stocks_highest_price[stock_code] = trade.price

        # import pdb; pdb.set_trace()

        # 关闭交易，相当于卖出
        if trade.status == Trade.Closed:
            self.current_stocks_highest_price.pop(stock_code)
