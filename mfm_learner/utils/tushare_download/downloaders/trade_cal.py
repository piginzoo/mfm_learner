import logging

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.default_downloader import DefaultDownloader

logger = logging.getLogger(__name__)


class TradeCalendar(DefaultDownloader):

    def get_table_name(self):
        return "trade_cal"

    def get_func(self):
        return self.pro.trade_cal


# python -m mfm_learner.utils.tushare_download.downloaders.trade_cal
if __name__ == '__main__':
    utils.init_logger()
    downloader = TradeCalendar()
    downloader.download()
