import datetime
import logging

import pandas as pd

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.periodly_downloader import PeriodlyDownloader

logger = logging.getLogger(__name__)


class IndexWeight(PeriodlyDownloader):

    def __init__(self, index_codes):
        super().__init__()
        self.index_codes = index_codes

    def get_table_name(self):
        return "index_weight"

    def get_date_column_name(self):
        return "trade_date"

    # 不行，年还是范围太大，我观察，1那年有5000+，所以还是超级录了，改为每月
    def get_period(self):
        return 'month'

    def download(self):
        df_all = []

        for index_code in self.index_codes:
            # 这里需要增加一个where条件，逐个指数来下载，这样做的原因是因为可能会后续追加其他指数
            df = self.download(save=False,
                                where=f"index_code='{index_code}'",
                                index_code=index_code)
            # 由于各个指数不一致，分别保存
            self.save(f'{self.get_table_name()}_{index_code}_{start_date}_{end_date}.csv', df)
            logger.debug("下载完 指数成分数据，[%s] %s~%s %d条 ...", index_code, start_date, end_date, len(df))

            df_all.append(df)

        # 合并并保存
        df_all = pd.concat(df_all)
        logger.debug("下载完所有指数成分数据 %r %d 条", self.index_codes, len(df_all))
        self.to_db(df_all)


# python -m mfm_learner.utils.tushare_download.downloaders.index_weight
if __name__ == '__main__':
    utils.init_logger()
    downloader = IndexWeight(["000001.SH", "000905.SH", "000300.SH", "000016.SH"])
    downloader.download()
