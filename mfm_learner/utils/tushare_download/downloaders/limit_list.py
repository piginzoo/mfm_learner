import logging

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.periodly_downloader import PeriodlyDownloader

logger = logging.getLogger(__name__)


class LimitList(PeriodlyDownloader):
    """
    数据太多，按照月份下载
    靠！tushare的数据是2016.2才开始有的
    """

    def get_func(self):
        return self.pro.limit_list

    def get_table_name(self):
        return "limit_list"

    def get_date_column_name(self):
        return "trade_date"

    def get_period(self):
        return 'month'


# python -m mfm_learner.utils.tushare_download.downloaders.limit_list
if __name__ == '__main__':
    utils.init_logger()
    downloader = LimitList()
    downloader.download()
