import datetime
import logging

import pandas as pd

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.base_downloader import BaseDownloader

logger = logging.getLogger(__name__)


class CodeDateDownloader(BaseDownloader):
    """
    这个用于下载限定了股票代码和时间范围的下载，
    主要用于各类指数数据的下载
    """

    def __init__(self, codes):
        super().__init__()
        self.codes = codes

    def download(self):
        df_all = []
        for code in self.codes:
            # 这里需要增加一个where条件，逐个code来下载，这样做的原因是因为可能会后续追加其他code
            start_date = self.get_start_date(where=f"ts_code='{code}'")
            end_date = utils.date2str(datetime.datetime.now())

            df = self.retry_call(func=self.get_func(),
                                 start_date=start_date,
                                 end_date=end_date,
                                 ts_code=code)
            # 由于各个指数不一致，分别保存
            self.save(f'{self.get_table_name()}_{code}_{start_date}_{end_date}.csv', df)
            logger.debug("下载了[%s]的数据，%s~%s, %d条", code, start_date, end_date, len(df))

            df_all.append(df)

        df_all = pd.concat(df_all)
        logger.debug("合计了下载 %r %s~%s 的数据 %d 条",
                     self.codes,
                     start_date,
                     end_date,
                     len(df_all))

        self.to_db(df_all)
