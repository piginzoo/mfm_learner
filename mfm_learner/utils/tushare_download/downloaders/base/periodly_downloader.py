import datetime
import logging

import pandas as pd

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.downloaders.base.base_downloader import BaseDownloader

logger = logging.getLogger(__name__)


class PeriodlyDownloader(BaseDownloader):
    """
    给index_weight用的，他的数据量巨大，单独写一个给他啊，按照日期每月下载
    """

    def get_table_name(self):
        return "index_weight"

    def get_date_column_name(self):
        return "trade_date"

    def get_period(self):
        raise NotImplemented()

    def periodly_download(self, func, start_date, end_date, period, **kwargs):
        """
        :param index_code:
        :param start_date:
        :param end_date:
        :param period: 下载周期，year|month|day
        """

        # 不行，年还是范围太大，我观察，1那年有5000+，所以还是超级录了，改为每月
        if period == "year":
            durations = utils.get_yearly_duration(start_date, end_date)
        elif period == "month":
            durations = utils.get_monthly_duration(start_date, end_date)
        else:
            raise ValueError("无法识别的下载间隔值：" + period)

        # 按照start_date ~ end_date，每年下载一次
        df_all = []
        for start_date, end_date in durations:
            df = self.retry_call(func=func,
                                 start_date=start_date,
                                 end_date=end_date,
                                 **kwargs)
            df_all.append(df)
            logger.debug("下载了%s~%s的%d条数据", start_date, end_date, len(df))
        df_all = pd.concat(df_all)

        logger.debug("合计下载了 %s~%s %d 条数据", start_date, end_date, len(df_all))
        return df_all

    def download(self, save=True, where=None, **kwargs):
        # 这里需要增加一个where条件，逐个指数来下载，这样做的原因是因为可能会后续追加其他指数
        start_date = self.get_start_date(where)
        end_date = utils.date2str(datetime.datetime.now())

        # 按照周期下载
        df = self.periodly_download(func=self.get_func(),
                                    start_date=start_date,
                                    end_date=end_date,
                                    period=self.get_period(),
                                    **kwargs)  # week | month

        logger.debug("下载完 %s 数据，%s~%s %d条 ...", self.get_table_name(), start_date, end_date, len(df))

        if save:
            # 由于各个指数不一致，分别保存
            self.save(f'{self.get_table_name()}_{start_date}_{end_date}.csv', df)
            self.to_db(df)

        return df
