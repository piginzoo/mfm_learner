import logging
import tushare
from utils import utils
from utils.tushare_download.downloaders.base.batch_downloader import BatchDownloader

logger = logging.getLogger(__name__)


class Daily(BatchDownloader):

    def __init__(self, adjust='hfq'):
        super().__init__()
        self.adjust = adjust
        self.table_name = "daily_{}".format(adjust)

    def download(self):
        return self.optimized_batch_download(func=tushare.pro_bar,
                                             multistocks=False,
                                             adj=self.adjust)

    def get_table_name(self):
        return self.table_name

    def get_date_column_name(self):
        return "trade_date"


# python -m utils.tushare_download.downloaders.daily
if __name__ == '__main__':
    utils.init_logger()
    downloader = Daily()
    downloader.download()
