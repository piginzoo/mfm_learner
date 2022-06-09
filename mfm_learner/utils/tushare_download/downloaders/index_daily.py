import datetime
import logging

import pandas as pd

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.base_downloader import BaseDownloader

logger = logging.getLogger(__name__)


class IndexDaily(BaseDownloader):

    def __init__(self, index_codes):
        super().__init__()
        self.index_codes = index_codes

    def get_table_name(self):
        return "index_daily"

    def get_date_column_name(self):
        return "trade_date"

    def download(self):
        df_all = []
        for index_code in self.index_codes:
            # 这里需要增加一个where条件，逐个指数来下载，这样做的原因是因为可能会后续追加其他指数
            start_date = self.get_start_date(where=f"ts_code='{index_code}'")
            end_date = utils.date2str(datetime.datetime.now())

            df = self.retry_call(func=self.pro.index_daily,
                                 start_date=start_date,
                                 end_date=end_date,
                                 ts_code=index_code)
            # 由于各个指数不一致，分别保存
            self.save(f'{self.get_table_name()}_{index_code}_{start_date}_{end_date}.csv',df)
            logger.debug("下载了指数[%s]每日数据，%s~%s, %d条", index_code, start_date, end_date,len(df))

            df_all.append(df)


        df_all = pd.concat(df_all)
        logger.debug("合计了下载指数 [%r] %s~%s 的日交易数据index_daily %d 条",
                     self.index_codes,
                     start_date,
                     end_date,
                     len(df_all))

        self.to_db(df_all)


# python -m mfm_learner.utils.tushare_download.downloaders.index_daily
if __name__ == '__main__':
    utils.init_logger()
    downloader = IndexDaily(["000001.SH", "000905.SH", "000300.SH", "000016.SH"])
    downloader.download()
