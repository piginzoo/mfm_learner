import time

from mfm_learner.utils import utils

from mfm_learner.utils.tushare_download.downloaders.stock_basic import StockBasic

from mfm_learner.utils.tushare_download.downloaders.balancesheet import BalanceSheet
from mfm_learner.utils.tushare_download.downloaders.cashflow import CashFlow
from mfm_learner.utils.tushare_download.downloaders.daily import Daily
from mfm_learner.utils.tushare_download.downloaders.daily_basic import DailyBasic
from mfm_learner.utils.tushare_download.downloaders.daily_hfq import DailyHFQ
from mfm_learner.utils.tushare_download.downloaders.fina_indicator import FinanceIndicator
from mfm_learner.utils.tushare_download.downloaders.income import Income
from mfm_learner.utils.tushare_download.downloaders.index_daily import IndexDaily
from mfm_learner.utils.tushare_download.downloaders.index_weekly import IndexWeekly
from mfm_learner.utils.tushare_download.downloaders.index_weight import IndexWeight
from mfm_learner.utils.tushare_download.downloaders.limit_list import LimitList
from mfm_learner.utils.tushare_download.downloaders.stk_holdernumber import StockHolderNumber
import pandas as pd
import logging

logger = logging.getLogger(__name__)

daily_data = [
    Daily(),
    DailyHFQ(),
    DailyBasic(),
    IndexDaily(["000001.SH", "000905.SH", "000300.SH", "000016.SH"]),
    IndexWeight(["000001.SH", "000905.SH", "000300.SH", "000016.SH"]),
    IndexWeekly(["000001.SH", "000905.SH", "000300.SH", "000016.SH"]),
    StockHolderNumber(),
    LimitList(),
]

quarter_date = [
    FinanceIndicator(),
    BalanceSheet(),
    Income(),
    CashFlow(),
    BalanceSheet()
]

db_engine = utils.connect_db()


def load(data):
    start_time = time.time()
    sql = f'select * from {data.get_table_name()}'
    df = pd.read_sql(sql, db_engine)
    logger.info("加载完 %s 表，用时: %.2f秒",data.get_table_name(),time.time() - start_time)
    return df


def check_all(df_basic):
    check_data(daily_data, df_basic, 12)
    # check_data(daily_data,4)


def check_data(datas, df_basic, year_count):
    df_basic.list_date

    for data in datas:
        df = load(data)
        df = df.rename(columns={'ann_date': 'trade_date'})
        df = df.sort_values(['ts_code', 'trade_date'])
        df.groupby('ts_code').apply(lambda x: check(x, df_basic, year_count))


def check(df_stock, df_basic, year_count):

    list_date = df_basic[df_basic.ts_code == df_stock.name].list_date
    if len(list_date)==0:
        logger.error("无法找到股票[%s]的上市日期,basic表中数据缺失",df_stock.name)
        list_date = '20080101'
    else:
        list_date = list_date.iloc[0]
        # 我们的数据是从2008之后的
        if list_date < '20080101': list_date = '20080101'

    start_date = df_stock.iloc[0].trade_date

    if start_date[:6] != list_date[:6]:
        logger.error("股票[%s]数据开始月份[%s]!=上市月份[%s]，数据可能缺失",
                     df_stock.name,
                     start_date,
                     list_date)
    # 先抽取月，再按年统计个数
    df_count = df_stock.trade_date. \
        apply(lambda x: x[:6]). \
        drop_duplicates(keep='first'). \
        apply(lambda x: x[:4]).value_counts()

    df_error = df_count[df_count.values != year_count]
    # 正常
    if len(df_error) == 1 and df_error.index == '2022':
        return True
    if len(df_error) == 0:
        return True

    logger.error("股票[%s]的数据有问题，不正常的年月：\n%r",df_stock.name, df_error)
    return False


# python -m mfm_learner.utils.data_checker
if __name__ == '__main__':
    utils.init_logger()
    df_basic = load(StockBasic())
    check_all(df_basic)
