# python -m utils.tushare_download.stock_company
from utils import utils
from utils.tushare_download.downloaders.daily_basic import DailyBasic
from utils.tushare_download.downloaders.stock_basic import StockBasic
from utils.tushare_download.downloaders.stock_company import StockCompany

DOWNLOADS=[
    StockCompany,
    DailyBasic,
    StockBasic,
]

if __name__ == '__main__':
    utils.init_logger()
    downloader = StockCompany()
    downloader.download()