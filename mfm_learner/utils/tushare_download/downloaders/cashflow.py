import logging

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.batch_stocks_downloader import BatchStocksDownloader

logger = logging.getLogger(__name__)


class CashFlow(BatchStocksDownloader):
    """现金流量表"""

    def __init__(self):
        super().__init__()
        self.multistocks = False

    def get_func(self):
        return self.pro.cashflow

    def get_table_name(self):
        return "cashflow"

    def get_date_column_name(self):
        return "ann_date"


"""
python -m mfm_learner.utils.tushare_download.downloaders.cashflow \
-f ../mlstock/data/stocks.txt
"""

if __name__ == '__main__':
    utils.init_logger()
    downloader = CashFlow()
    downloader.download()
