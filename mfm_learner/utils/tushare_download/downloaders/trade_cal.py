import logging

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.base_downloader import BaseDownloader

logger = logging.getLogger(__name__)


class TradeCalendar(BaseDownloader):

    def get_table_name(self):
        return "trade_cal"

    def get_date_column_name(self):
        return "cal_date"

    def download(self):
        df = self.pro.trade_cal()

        logger.debug("下载交易日信息 [%d]条", len(df))

        # 数据量不大，直接全部重新下载，replace数据库中的数据
        self.to_db(df, if_exists='replace')


# python -m mfm_learner.utils.tushare_download.downloaders.trade_cal
if __name__ == '__main__':
    utils.init_logger()
    downloader = TradeCalendar()
    downloader.download()
