import logging

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.batch_stocks_downloader import BatchStocksDownloader

logger = logging.getLogger(__name__)


class Income(BatchStocksDownloader):
    """利润表"""

    def __init__(self):
        super().__init__()
        self.multistocks = False

    def get_func(self):
        return self.pro.income

    def get_table_name(self):
        return "income"

    def get_date_column_name(self):
        return "ann_date"

"""
python -m mfm_learner.utils.tushare_download.downloaders.income \
-f ../mlstock/data/stocks.txt
"""
if __name__ == '__main__':
    utils.init_logger()
    downloader = Income()
    downloader.download()
