import logging

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.batch_stocks_downloader import BatchStocksDownloader

logger = logging.getLogger(__name__)


class MoneyFlow(BatchStocksDownloader):

    def __init__(self):
        super().__init__()
        self.multistocks = False

    def get_func(self):
        return self.pro.moneyflow

    def get_table_name(self):
        return "moneyflow"

    def get_date_column_name(self):
        return "trade_date"


# python -m mfm_learner.utils.tushare_download.downloaders.moneyflow
if __name__ == '__main__':
    utils.init_logger()
    downloader = MoneyFlow()
    downloader.download()
