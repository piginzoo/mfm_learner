import logging

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.batch_stocks_downloader import BatchStocksDownloader

logger = logging.getLogger(__name__)

fields = 'ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv'


class DailyBasic(BatchStocksDownloader):

    def get_func(self):
        return self.pro.daily_basic

    def get_table_name(self):
        return "daily_basic"

    def get_date_column_name(self):
        return "trade_date"


# python -m mfm_learner.utils.tushare_download.downloaders.daily_basic
if __name__ == '__main__':
    utils.init_logger()
    downloader = DailyBasic()
    downloader.download()
