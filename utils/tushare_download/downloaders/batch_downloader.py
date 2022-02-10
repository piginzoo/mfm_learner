import datetime
import logging
import math
import time

import numpy as np
import pandas as pd
from tqdm import tqdm

import utils.utils
from utils import utils
from utils.tushare_download.downloaders.base_downloader import BaseDownload

logger = logging.getLogger(__name__)

fields = 'ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv'
TRADE_DAYS_PER_YEAR = 252  # 1年的交易日
MAX_RECORDS = 4800  # 最多一次的下载行数，tushare是5000，稍微降一下到4800


class BatchDownloader(BaseDownload):
    """
    用于下载所有的股票，一只一只股票的，
    可以支持1只，
    也可以支持多只一起批量下载（为了优化），多只时，要算一下到每次下载多少只股票最合适，
    是按照每年252个交易日来计算的记录数。
    """

    def get_stock_codes(self):
        df = pd.read_sql('select * from stock_basic', self.db_engine)
        return df['ts_code']

    def calculate_best_fetch_stock_num(self, start_date, end_date):
        """
        计算最多可以下载多少只股票:
        4800/一只股票的条数，
        1只股票条数=天数/365*252
        """
        delta = utils.str2date(end_date) - utils.str2date(start_date)
        days = delta.days
        record_num_per_stock = math.floor(days * TRADE_DAYS_PER_YEAR / 365)
        stock_num = math.floor(MAX_RECORDS / record_num_per_stock)
        logger.debug("下载优化:共%d天,每只股票%d条,每次下载4800条，所以，可以一次可下载%d只股票", days, record_num_per_stock, stock_num)
        return stock_num

    def optimized_batch_download(self, func, multistocks, **kwargs):
        """
        使用优化完的参数，来下载股票，一次可以支持1只或多只，由参数multistocks决定。

        支持多只的时候，需要使用函数calculate_best_fetch_stock_num，计算到底一次下载几只最优

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
        self.to_db(df_all)
