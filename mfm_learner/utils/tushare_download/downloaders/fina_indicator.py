import logging

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.batch_stocks_downloader import BatchStocksDownloader

logger = logging.getLogger(__name__)


class FinanceIndicator(BatchStocksDownloader):

    def __init__(self):
        super().__init__()

    def get_func(self):
        return self.pro.fina_indicator

    def get_table_name(self):
        return "fina_indicator"

    def get_date_column_name(self):
        return "ann_date"


# python -m mfm_learner.utils.tushare_download.downloaders.fina_indicator
if __name__ == '__main__':
    utils.init_logger()
    downloader = FinanceIndicator()
    downloader.download()
