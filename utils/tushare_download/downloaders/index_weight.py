import datetime
import logging

import pandas as pd
import tushare as ts

from utils import utils
from utils.tushare_download.downloaders.base_downloader import BaseDownload

logger = logging.getLogger(__name__)



class IndexWeight(BaseDownload):

    def __init__(self, index_code, start_date, end_date):
        super().__init__()
        self.index_code = index_code
        self.start_date = start_date
        self.end_date = end_date

    def get_table_name(self):
        return "index_weight"

    def get_date_column_name(self):
        return "trade_date"



    def download(self):
        start_date = self.get_start_date()
        end_date = utils.date2str(datetime.datetime.now())

        # 不行，年还是范围太大，我观察，1那年有5000+，所以还是超级录了，改为每月
        # durations = utils.get_yearly_duration(start_date, end_date)
        durations = utils.get_monthly_duration(start_date, end_date)

        # 按照start_date ~ end_date，每年下载一次
        df_all = []
        for start_date, end_date in durations:
            df = self.retry_call(func=self.pro.index_weight,
                                 index_code=self.index_code,
                                 start_date=start_date,
                                 end_date=end_date
                                 )
            df_all.append(df)
            logger.debug("下载了%s~%s的%d条指数[%s]成分数据",start_date,end_date,len(df),self.index_code)
        df_all = pd.concat(df_all)

        logger.debug("下载了指数 [%s] %s~%s 的index_weight %d 条",
                     self.index_code,
                     self.start_date,
                     self.end_date,
                     len(df_all))

        self.to_db(df_all)


# python -m utils.tushare_download.downloaders.index_weight
if __name__ == '__main__':
    utils.init_logger()
    downloader = IndexWeight("000905.SH", "20070101", "20220101")
    downloader.download()
