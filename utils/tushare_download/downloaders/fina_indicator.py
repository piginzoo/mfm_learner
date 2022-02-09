import logging

from utils import utils
from utils.tushare_download.downloaders.batch_downloader import BatchDownloader

logger = logging.getLogger(__name__)


class FinanceIndicator(BatchDownloader):

    def __init__(self):
        super().__init__()

    def download(self):
        return self.optimized_batch_download(func=self.pro.fina_indicator)

    def get_table_name(self):
        return "fina_indicator"

    def get_date_column_name(self):
        return "anna_date"


# python -m utils.tushare_download.downloaders.fina_indicator
if __name__ == '__main__':
    utils.init_logger()
    downloader = FinanceIndicator()
    downloader.download()
