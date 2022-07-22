import logging
import tushare
from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.batch_stocks_downloader import BatchStocksDownloader

logger = logging.getLogger(__name__)


class DailyHFQ(BatchStocksDownloader):
    """
    下载每日后复权数据，用的是pro_bar接口，为何没有用daily接口呢？
    因为daily接口不带后复权数据，只有pro_bar可以提供。
    但是，pro_bar有个很恶心的地方，只能一支一支股票的调用，
    并且，tushare不支持多进程，所以，就变得很恶心了，只能循环着调用全市场将近500只股票。
    实测，5000只，1个月的数据下载，需要20多分钟。
    """

    def __init__(self, adjust='hfq'):
        super().__init__()
        self.adjust = adjust
        self.table_name = "daily_{}".format(adjust)
        self.multistocks = False  # <-- 必须False，我血的教训，pro_bar不支持多个股票，传入多个会导致重复数据

    def get_func(self):
        return tushare.pro_bar

    def get_func_kwargs(self):
        return {'adj': self.adjust}

    def get_table_name(self):
        return self.table_name

    def get_date_column_name(self):
        return "trade_date"


# python -m mfm_learner.utils.tushare_download.downloaders.daily_hfq
if __name__ == '__main__':
    utils.init_logger()
    downloader = DailyHFQ()
    downloader.download()
