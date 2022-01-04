from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import backtrader as bt  # 引入backtrader框架
import backtrader.analyzers as btay  # 添加分析函数
import pandas as pd
import tushare as ts

from example import factor_combiner
from utils import tushare_utils

"""
用factor_tester.py中合成的多因子，做选择股票的策略 ，去选择中证500的股票，跑收益率回测。使用backtrader来做回测框架。
参考：
- https://zhuanlan.zhihu.com/p/351751730
"""


class Percent(bt.Sizer):
    """
    定义每次卖出的股票的百分比
    """
    params = (
        ('percents', 10),
        ('retint', False),  # 返回整数
    )

    def __init__(self):
        pass

    def _getsizing(self, comminfo, cash, data, isbuy):
        position = self.broker.getposition(data)
        if not position:
            size = cash / data.close[0] * (self.params.percents / 100)
        else:
            size = position.size

        if self.p.retint:
            size = int(size)

        return size


class CombineFactorStrategy(bt.Strategy):
    """
    我自己的多因子策略，即，用我的多因子来进行选股，股票池是中证500，每一个选股周期，我都要根据当期数据，去计算因子暴露（因子值），
    然后根据因子值，对当前期中证500股票池中的股票进行排序，（这个期间中证500可能备选股票可能会变化）
    然后，选择前100名进行投资，对比新旧100名中，卖出未在list中，买入list中的，如此进行3年、5年投资，看回测收益率。
    -------------
    实现细节：
    - 回测期间使用2014.1.1~2018.12.31，跨度5年，主要是想看看，2015年股灾、2016年之后风格轮动后的效果对比，
    - 后续的期间，是可以使用之前的数据了，比如回测的是2017.3的，就可以使用2017.2月的数据了
    - 每次选股前，都要重新计算因子暴露（敞口、因子值），所以计算量还不小呢，即每次都要用一个滑动窗口（12个月）算
    - 单因子有负向的，比如市值因子，那么在因子合成的时候，应该怎么处理负向的呢？
    """

    # 可配置策略参数
    params = dict(
        period=30,  # 均线周期
        stake=100,  # 单笔交易股票数目
    )

    def __init__(self,stock_index):
        self.stock_index = stock_index

    def next(self):
        """
        每天都会回调，我们的逻辑是：
        - 是否到达调仓周期，如果未到忽略
        - 找出此期间中证500只包含的股票池中的股票
        - 根据每支股票的当日的数据，计算每一个单因子值
        - 讲多个因子值，合称为一个因子
        - 根据每只股票的合成因子值排序，找出最好的100只备选股
        - 卖出持仓的，却不在备选股中的那些股票
        - 买入那些在备选股中，却不在持仓中的股票
        异常处理：
        - 如果股票停盘，会顺序买入下一位排名的股票
        - 每次都是满仓，即用卖出的股票头寸，全部购入新的股票，头寸仅在新购入股票中平均分配
        - 如果没有头寸，则不再购买（这种情况应该不会出现）
        """

        stock_codes = tushare_utils.index_weight(self.stock_index, start_date)
        combined_factor = factor_combiner.synthesize_by_jaqs(stock_codes, start, end)
        # 回测最后一天不进行买卖
        if self.datas[0].datetime.date(0) == end_date:
            return

            # for i, d in enumerate(self.datas):
        #     pos = self.getposition(d)
        #     if not len(pos):
        #         if d.close[0] > self.inds[d][0]:  # 达到买入条件
        #             self.buy(data=d, size=self.p.stake)
        #     elif d.close[0] < self.inds[d][0]:  # 达到卖出条件
        #         self.sell(data=d)


def main(start_date, end_date, index, stock_num):
    cerebro = bt.Cerebro()  # 初始化cerebro

    stock_codes = tushare_utils.index_weight(index)

    # 加载股票池中的所有的交易数据
    def get_stock_data(code, d1, d2):  # 此处时间应与下面回测期间一致
        pro = ts.pro_api()
        df = ts.pro_bar(ts_code=code, adj='qfq', start_date=d1, end_date=d2)
        df.index = pd.to_datetime(df.trade_date)
        df.sort_index(ascending=True, inplace=True)
        df = df.rename(columns={'vol': 'volume'})
        df['openinterest'] = 0
        df = df[['open', 'high', 'low', 'close', 'volume', 'openinterest']]
        return df

    for stock_code in stock_codes:
        # 获取数据
        dataframe = get_stock_data(stock_code, start_date, end_date)
        # 加载数据,每只股票的交易数据加载一次
        data = bt.feeds.PandasData(dataname=dataframe, fromdate=start, todate=end)
        # 在Cerebro中添加股票数据
        cerebro.adddata(data, name=stock_code)

    ################## cerebro整体设置 #####################

    # 设置启动资金
    startcash = 100000.0
    cerebro.broker.setcash(startcash)

    # https://baike.baidu.com/item/%E8%82%A1%E7%A5%A8%E4%BA%A4%E6%98%93%E6%89%8B%E7%BB%AD%E8%B4%B9/9080806
    # 设置交易手续费：印花税0.001+证管费0.00002+证券交易经手费0.0000487+券商交易佣金0.003
    cerebro.broker.setcommission(commission=0.004)

    # 设置订单份额,设置每笔交易交易的股票数量,比如" cerebro.addsizer(bt.sizers.FixedSize, stake=10)"
    # 告诉平台，我们是每次买卖股票数量固定的，stake=10就是10股。
    # 实际过程中，我们不可能如此简单的制定买卖的数目，而是要根据一定的规则，这就需要自己写一个sizers
    cerebro.addsizer(Percent)

    # 将交易策略加载到回测系统中
    cerebro.addstrategy(CombineFactorStrategy)

    # 添加分析对象
    cerebro.addanalyzer(btay.SharpeRatio, _name="sharpe")  # 夏普指数
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')  # 回撤分析

    # 打印
    print(f'回测期间：{d1}:{d2}\n初始资金: {startcash}')
    # 运行回测
    results = cerebro.run()
    # 打印最后结果
    portvalue = cerebro.broker.getvalue()
    pnl = portvalue - startcash
    # 打印结果
    print(f'总资金: {round(portvalue, 2)}')
    print(f'净收益: {round(pnl, 2)}')
    print("夏普比例:", results[0].analyzers.sharpe.get_analysis())
    print("回撤", results[0].analyzers.DW.get_analysis())

    cerebro.plot(style="candlestick")  # 绘图


if __name__ == '__main__':
    start = "20200101"  # 开始日期
    end = "20201201"  # 结束日期
    index = '000905.SH'  # 股票池为中证500
    period = 20  # 调仓周期
    stock_num = 10  # 用股票池中的几只，初期调试设置小10，后期可以调成全部
    main(start, end, index, stock_num)
