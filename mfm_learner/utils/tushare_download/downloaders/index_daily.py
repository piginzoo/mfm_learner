import datetime
import logging

import pandas as pd

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.base_downloader import BaseDownloader
from mfm_learner.utils.tushare_download.downloaders.base.code_date_downloader import CodeDateDownloader

logger = logging.getLogger(__name__)


class IndexDaily(CodeDateDownloader):

    def get_table_name(self):
        return "index_daily"

    def get_date_column_name(self):
        return "trade_date"

    def get_func(self):
        return self.pro.index_daily


# python -m mfm_learner.utils.tushare_download.downloaders.index_daily
if __name__ == '__main__':
    utils.init_logger()
    # 上证指数、中证500、沪深300、
    downloader = IndexDaily(["000001.SH", "000905.SH", "000300.SH", "000016.SH"])
    downloader.download()
