"""
用来，一键更新所有的新数据到本地，省的每个还都得再下载
"""
from mfm_learner.utils.tushare_download.downloaders.index_weekly import IndexWeekly

from mfm_learner.utils.tushare_download.downloaders.cashflow import CashFlow

from mfm_learner.utils.tushare_download.downloaders.income import Income

from mfm_learner.utils.tushare_download.downloaders.balancesheet import BalanceSheet

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.daily import Daily
from mfm_learner.utils.tushare_download.downloaders.daily_basic import DailyBasic
from mfm_learner.utils.tushare_download.downloaders.daily_hfq import DailyHFQ
from mfm_learner.utils.tushare_download.downloaders.fina_indicator import FinanceIndicator
from mfm_learner.utils.tushare_download.downloaders.index_daily import IndexDaily
from mfm_learner.utils.tushare_download.downloaders.index_weight import IndexWeight
from mfm_learner.utils.tushare_download.downloaders.stk_holdernumber import StockHolderNumber
from mfm_learner.utils.tushare_download.downloaders.stock_basic import StockBasic
from mfm_learner.utils.tushare_download.downloaders.stock_company import StockCompany
from mfm_learner.utils.tushare_download.downloaders.trade_cal import TradeCalendar
import time, datetime, logging

logger = logging.getLogger(__name__)


def main():
    TradeCalendar().download()
    StockCompany().download()
    StockBasic().download()
    Daily().download()
    DailyHFQ().download()
    DailyBasic().download()
    IndexDaily(["000001.SH", "000905.SH", "000300.SH", "000016.SH"]).download()


def download_all():
    start = time.time()

    main()

    # 目前这2个数据不需要使用
    FinanceIndicator().download()
    IndexWeight(["000001.SH", "000905.SH", "000300.SH", "000016.SH"]).download()
    BalanceSheet().download()
    Income().download()
    CashFlow().download()
    BalanceSheet().download()
    IndexWeekly(["000001.SH", "000905.SH", "000300.SH", "000016.SH"]).download()
    StockHolderNumber().downlaod()

    logger.debug("下载所有的最新数据，共耗时: %s ", str(datetime.timedelta(seconds=time.time() - start)))


# python -m mfm_learner.utils.tushare_download.updator
if __name__ == '__main__':
    utils.init_logger()
    download_all()
