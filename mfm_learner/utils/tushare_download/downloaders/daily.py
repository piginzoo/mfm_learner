import logging
import tushare
from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.batch_downloader import BatchDownloader

logger = logging.getLogger(__name__)


class Daily(BatchDownloader):

    def __init__(self, adjust='hfq'):
        super().__init__()
        self.adjust = adjust
        self.table_name = "daily_{}".format(adjust)

    def download(self):
        return self.optimized_batch_download(func=tushare.pro_bar,
                                             multistocks=False, #<-- 必须False，我血的教训，pro_bar不支持多个股票，传入多个会导致重复数据
                                             adj=self.adjust)

    def get_table_name(self):
        return self.table_name

    def get_date_column_name(self):
        return "trade_date"


# python -m mfm_learner.utils.tushare_download.downloaders.daily
if __name__ == '__main__':
    utils.init_logger()
    downloader = Daily()
    downloader.download()
