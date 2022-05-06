"""
用来，一键更新所有的新数据到本地，省的每个还都得再下载
"""
from utils import utils
from utils.tushare_download.downloaders.daily import Daily
from utils.tushare_download.downloaders.daily_basic import DailyBasic
from utils.tushare_download.downloaders.fina_indicator import FinanceIndicator
from utils.tushare_download.downloaders.index_daily import IndexDaily
from utils.tushare_download.downloaders.index_weight import IndexWeight
from utils.tushare_download.downloaders.trade_cal import TradeCalendar


def main():
    # Daily().download()
    # DailyBasic().download()
    FinanceIndicator().download()
    IndexDaily().download()
    IndexWeight().download()
    TradeCalendar().download()


# python -m utils.tushare_download.updator
if __name__ == '__main__':
    utils.init_logger()
    main()
