import logging

import talib
from backtrader.feeds import PandasData

from datasource import datasource_factory, datasource_utils
from example import factor_utils
from utils import utils

logger = logging.getLogger(__name__)

datasource = datasource_factory.get()


# 按照backtrader要求的数据格式做数据整理,格式化成backtrader要求：索引日期；列名叫vol和datetime
def comply_backtrader_data_format(df):
    df = df.rename(columns={'vol': 'volume'})  # 列名准从backtrader的命名规范
    df['openinterest'] = 0  # backtrader需要这列，所以给他补上
    df = datasource_utils.reset_index(df, date_only=True)  # 只设置日期列为索引
    df = df.sort_index(ascending=True)
    return df


# 修改原数据加载模块，以便能够加载更多自定义的因子数据
class StockData(PandasData):
    lines = ('atr',)
    params = (('atr', -1),)


# 自定义数据
# 参考：https://community.backtrader.com/topic/1676/dynamically-set-params-and-lines-attribute-of-class-pandasdata/8
def create_data_feed_class(factor_names):
    """
    需要用动态创建类的方式，来动态扩展bakctrader的PandasData，
    原因是他的这个类狠诡异，要么就在类定义的时候就定义params，
    否则，你不太可能在构造函数啥的里面去动态扩展params，
    但是我们不同的factors可能数量不同，为了要合并到PandasData的列中，
    就需要这种动态方式，参考的是backtrader上例子。
    """

    lines = tuple(factor_names)
    params = tuple([(factor_name, -1) for factor_name in factor_names])
    return type('PandasDataFeed', (PandasData,), {'lines': lines, 'params': params})


def load_stock_data(cerebro, start_date, end_date, stock_codes,atr_period):
    # 想脑波cerebro逐个追加每只股票的数据
    for stock_code in stock_codes:
        df_stock = datasource.daily(stock_code, start_date, end_date)

        df_stock['atr'] = talib.ATR(df_stock.high.values,df_stock.low.values,df_stock.close.values, timeperiod=atr_period)

        trade_days = datasource.trade_cal(start_date, end_date)
        if len(df_stock) / len(trade_days) < 0.6:
            logger.warning("股票[%s] 缺失交易日[%d/总%d]天，超40%%，忽略此股票",
                           stock_code, len(df_stock), len(trade_days))
            continue
        df_stock = comply_backtrader_data_format(df_stock)

        d_start_date = utils.str2date(start_date)  # 开始日期
        d_end_date = utils.str2date(end_date)  # 结束日期

        # plot=False 不在plot图中显示个股价格
        data = StockData(dataname=df_stock, fromdate=d_start_date, todate=d_end_date, plot=False)
        cerebro.adddata(data, name=stock_code)
        logger.debug("初始化股票[%s]数据到脑波cerebro：%d 条", stock_code, len(df_stock))



def load_data_deperate(cerebro, start_date, end_date, stock_codes, factor_names):
    """
    @deperate 废弃,之前是把因子并入股票数据的列后面
    :param cerebro:
    :param start_date:
    :param end_date:
    :param stock_codes:
    :param factor_names:
    :return:
    """
    PandasDataClass = create_data_feed_class(factor_names)

    # 想脑波cerebro逐个追加每只股票的数据
    for stock_code in stock_codes:
        df_stock = datasource.daily(stock_code, start_date, end_date)

        trade_days = datasource.trade_cal(start_date, end_date)
        if len(df_stock) / len(trade_days) < 0.9:
            logger.warning("股票[%s] 缺失交易日[%d/总%d]天，超过10%%，忽略此股票",
                           stock_code, len(df_stock), len(trade_days))
            continue

        # 获得1只股票的多个因子数据：index(datetime,code), value(因子)
        df_factors = factor_utils.get_factor(factor_names, stock_code, start_date, end_date)
        df_factors.columns = [f"factor_{c}" for c in df_factors.columns]
        df_factors.reset_index(df_factors)

        df_stock = df_stock.merge(df_factors, on=['code', 'datetime'], how="inner")

        df_stock = comply_backtrader_data_format(df_stock)

        d_start_date = utils.str2date(start_date)  # 开始日期
        d_end_date = utils.str2date(end_date)  # 结束日期

        # plot=False 不在plot图中显示个股价格
        data = PandasDataClass(dataname=df_stock, fromdate=d_start_date, todate=d_end_date, plot=False)
        cerebro.adddata(data, name=stock_code)
        logger.debug("初始化股票[%s]数据到脑波cerebro：%d 条", stock_code, len(df_stock))
