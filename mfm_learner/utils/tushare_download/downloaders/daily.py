import logging
import tushare
from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.batch_downloader import BatchDownloader

logger = logging.getLogger(__name__)


class Daily(BatchDownloader):

    """
    下载每日原始交易数据（未复权数据）:
        https://tushare.pro/document/2?doc_id=27
    """

    def __init__(self):
        super().__init__()
        self.table_name = "daily"

    def download(self):
        return self.optimized_batch_download(func=self.pro.daily,multistocks=True)

    def get_table_name(self):
        return self.table_name

    def get_date_column_name(self):
        return "trade_date"


# python -m mfm_learner.utils.tushare_download.downloaders.daily
if __name__ == '__main__':
    utils.init_logger()
    downloader = Daily()
    downloader.download()
