"""
用来，一键更新所有的新数据到本地，省的每个还都得再下载
"""
from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.daily import Daily
from mfm_learner.utils.tushare_download.downloaders.daily_basic import DailyBasic
from mfm_learner.utils.tushare_download.downloaders.fina_indicator import FinanceIndicator
from mfm_learner.utils.tushare_download.downloaders.index_daily import IndexDaily
from mfm_learner.utils.tushare_download.downloaders.index_weight import IndexWeight
from mfm_learner.utils.tushare_download.downloaders.stock_basic import StockBasic
from mfm_learner.utils.tushare_download.downloaders.stock_company import StockCompany
from mfm_learner.utils.tushare_download.downloaders.trade_cal import TradeCalendar


def main():
    StockCompany().download()
    StockBasic().download()
    TradeCalendar().download()
    Daily().download()
    DailyBasic().download()
    FinanceIndicator().download()
    IndexDaily(["000001.SH","000905.SH", "000300.SH", "000016.SH"]).download()
    IndexWeight(["000001.SH","000905.SH", "000300.SH", "000016.SH"]).download()


# python -m mfm_learner.utils.tushare_download.updator
if __name__ == '__main__':
    utils.init_logger()
    main()
