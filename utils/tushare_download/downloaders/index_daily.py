import datetime
import logging

import pandas as pd

from utils import utils
from utils.tushare_download.downloaders.base_downloader import BaseDownload

logger = logging.getLogger(__name__)


class IndexDaily(BaseDownload):

    def __init__(self, index_codes):
        super().__init__()
        self.index_codes = index_codes

    def get_table_name(self):
        return "index_daily"

    def get_date_column_name(self):
        return "trade_date"

    def download(self):
        start_date = self.get_start_date()
        end_date = utils.date2str(datetime.datetime.now())

        df_all = []
        for index_code in self.index_codes:
            df = self.retry_call(func=self.pro.index_weight,
                                 index_code=index_code,
                                 start_date=start_date,
                                 end_date=end_date)
            df_all.append(df)
        df_all = pd.concat(df_all)
        logger.debug("下载了指数 [%s] %s~%s 的日交易数据index_daily %d 条",
                     self.index_code,
                     start_date,
                     end_date,
                     len(df_all))

        self.to_db(df_all)


# python -m utils.tushare_download.downloaders.index_daily
if __name__ == '__main__':
    utils.init_logger()
    downloader = IndexDaily(["000905.SH", "000300.SH", "000016.SH"])
    downloader.download()
