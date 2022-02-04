import datetime
import logging
import math
import time

import pandas as pd
import numpy as np
import utils.utils
from utils import utils
from utils.tushare_download.downloaders.base_downloader import BaseDownload
from tqdm import tqdm

logger = logging.getLogger(__name__)

fields = 'ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv'


class BatchDownloader(BaseDownload):
    """
    用于批量下载
    """

    def get_table_name(self):
        raise NotImplemented()

    def get_date_column_name(self):
        raise NotImplemented()

    def get_start_date(self):
        return '20210630'

        utils.str2date(latest_date)
        # TODO date +1

        table_name = self.get_table_name()
        date_column_name = self.get_date_column_name()
        df = pd.read_sql('select max({}) from {}'.format(date_column_name, table_name), self.db_engine)
        assert len(df) == 1
        latest_date = df.iloc[:, 0].item()
        logger.debug("数据库中表[%s]的最后日期[%s]为：%s", table_name, date_column_name, latest_date)
        return latest_date

    def optimized_batch_download(self, func, multistocks, **kwargs):
        """
        :param func: 调用的tushare的api的函数
        :param multistocks: 是否支持同时取多只股票,原因是pro_bar不支持：https://tushare.pro/document/2?doc_id=109
        :param kwargs:
        :return:
        """
        start_time = time.time()

        start_date = self.get_start_date()
        end_date = utils.date2str(datetime.datetime.now())
        stock_codes = self.get_stock_codes()

        logger.debug("准备下载 %s~%s, %d 只股票的基本信息", start_date, end_date, len(stock_codes))

        if multistocks:
            stock_num_once = self.calculate_best_fetch_stock_num(start_date, end_date)
            stock_codes = np.array_split(stock_codes, math.ceil(len(stock_codes) / stock_num_once))  # 定义几组
            stock_codes = [",".join(stocks) for stocks in stock_codes]
            logger.debug("支持多股票下载，共%d个批次，每批次%d只股票同时获取", len(stock_codes), stock_num_once)

        logger.debug("调用[%s]，共%d个批次，下载间隔%d毫秒",
                     self.get_table_name(),
                     len(stock_codes),
                     self.call_interval * 1000)
        df_all = []
        pbar = tqdm(total=len(stock_codes))
        for i, ts_code in enumerate(stock_codes):
            df = self.retry_call(func=func,
                                 ts_code=ts_code,
                                 start_date=start_date,
                                 end_date=end_date,
                                 **kwargs)
            df_all.append(df)
            # Tushare Exception: 抱歉，您每分钟最多访问该接口400次，
            # 权限的具体详情访问：https://tushare.pro/document/1?doc_id=108
            time.sleep(self.call_interval)
            pbar.update(i)
        pbar.close()

        df_all = pd.concat(df_all)

        csv_file_name = "{}_{}_{}.csv".format(self.get_table_name(), start_date, end_date)
        self.save(df=df_all, name=csv_file_name)

        logger.debug("下载了 %s~%s, %d只股票的%d条数据=>%s, %.2f秒",
                     start_date,
                     end_date,
                     len(stock_codes),
                     len(df_all),
                     csv_file_name,
                     time.time() - start_time)
        # self.to_db(df_new, "stock_basic")


# python -m utils.tushare_download.daily_basic
if __name__ == '__main__':
    utils.init_logger()
    downloader = DailyBasic()
    downloader.download()
