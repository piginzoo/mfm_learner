import logging

from utils import utils
from utils.tushare_download.downloaders.batch_downloader import BatchDownloader

logger = logging.getLogger(__name__)

fields = 'ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv'


class DailyBasic(BatchDownloader):

    def download(self):
        return self.optimized_batch_download(func=self.pro.daily_basic,
                                             multistocks=True,
                                             fields=fields)

    def get_table_name(self):
        return "daily_basic"

    def get_date_column_name(self):
        return "trade_date"


# python -m utils.tushare_download.daily_basic
if __name__ == '__main__':
    utils.init_logger()
    downloader = DailyBasic()
    downloader.download()
