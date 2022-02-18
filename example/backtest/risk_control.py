import logging

import numpy as np
import talib

logger = logging.getLogger(__name__)


class RiskControl():
    """
    实现多因子的风控策略，
    目前实现了两种：
    1、是单个持仓期间的股票，他是否下跌达到了N*ATR，就清仓
    2、整体价值（市值+现金）下跌了2*STD，后者是15%，就清仓
    """

    def __init__(self, strategy):
        self.strategy = strategy
        self.current_stocks_highest_price = {}
        self.portfolio_highest_value = self.strategy.broker.getvalue()  # 组合的最高价格
        self.portfolio_values = []  # 组合每天的市值
        logger.debug("创建风控对象：组合当前最高市值[%.2f]", self.portfolio_highest_value)

    def portfolio_risk_control(self):
        # 计算整个历史的方差
        std = np.array(self.portfolio_values).std()
        # 使用当天的开盘价，来计算风险，也就是，如果开市就出现异常，就需要卖出
        today_value = self.strategy.broker.getvalue()
        # 如果今天市值创新高，那么记录他即可
        if today_value > self.portfolio_highest_value:
            self.portfolio_highest_value = today_value
            return False

        # 计算回撤
        drawback = self.portfolio_highest_value - today_value

        # 如果回撤小于2倍STD，不理睬
        if np.isnan(std) or drawback < 2 * std:
            logger.debug("出现回撤[%.2f] < 2*波动率(标准差[%.2f])，无视总体风险", drawback, std)
            return False

        # 如果回撤大于2倍STD，就要整体清仓了
        for stock_code in self.strategy.current_stocks:
            self.strategy.sell_out(stock_code)

        logger.debug("回撤[%.2f] > 2*波动率(标准差[%.2f])，全部清仓%d只股票",
                     drawback,
                     std,
                     len(self.strategy.current_stocks))
        # 彻底退出回测
        self.strategy.env.runstop()
        logger.debug("全部清仓，退出整个回测交易！！！")
        return True

    def execute(self):
        """
        对持仓进行分线控制：
        风控规则：每天更新每支股票的历史最高价，如果当日回撤大于N倍ATR，则触发风控（清仓）
        返回：触发风控清仓的股票列表。
        """

        if self.portfolio_risk_control(): return

        exclude_stock_codes = []

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
            logger.debug("股票[%s]历史最高价[%.2f]当前价[%.2f]回撤[%.2f]ATR[%.2f]",
                         stock_code,
                         open_price,
                         highest_price,
                         highest_price - open_price,
                         self.strategy.atr_times * atr)
            if (highest_price - open_price) > self.strategy.atr_times * atr:
                logger.warning("股票[%s]的开盘价[%.2f]的回撤[%.2f]大于%d倍的ATR[%.2f]，触发风控清仓",
                               stock_code,
                               open_price,
                               highest_price - open_price,
                               self.strategy.atr_times,
                               atr)
                self.strategy.sell_out(stock_code)
                exclude_stock_codes.append(stock_code)
        return exclude_stock_codes


def update_atr(df, time_period=15):
    df['atr'] = talib.ATR(df.high.values, df.low.values, df.close.values, timeperiod=time_period)
    return df


def risk_control(df):
    """
    :param df:
    :return:
    """
    # 获得回撤
    drawdown = get_drawdown()
