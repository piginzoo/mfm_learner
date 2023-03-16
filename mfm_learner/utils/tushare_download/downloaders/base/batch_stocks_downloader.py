import argparse
import datetime
import logging
import math
import time

import numpy as np
import pandas as pd
from tqdm import tqdm

from mfm_learner.utils import utils
from mfm_learner.utils.tushare_download.conf import MAX_STOCKS_BATCH, TODAY_TIMING
from mfm_learner.utils.tushare_download.downloaders.base.base_downloader import BaseDownloader

logger = logging.getLogger(__name__)

fields = 'ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv'
TRADE_DAYS_PER_YEAR = 252  # 1年的交易日
MAX_RECORDS = 4800  # 最多一次的下载行数，tushare是5000，稍微降一下到4800


class BatchStocksDownloader(BaseDownloader):
    """
    用于下载所有的股票，一只一只股票的，
    可以支持1只，
    也可以支持多只一起批量下载（为了优化），多只时，要算一下到每次下载多少只股票最合适，
    是按照每年252个交易日来计算的记录数。
    """

    def __init__(self):
        super().__init__()
        self.multistocks = True

    def download(self):
        start = time.time()

        # 看看参数中是否需要提供股票的列表，如果有，仅下载这些股票，否则下载所有（None)
        stock_codes = self.get_stocks_from_argument()

        self.optimized_batch_download(func=self.get_func(),
                                      stock_codes=stock_codes,
                                      multistocks=self.multistocks)
        logger.debug("共耗时: %s ", str(datetime.timedelta(seconds=time.time() - start)))

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
        logger.debug("需要下载%d天的数据", days)

        if days == 0:
            return 0

        record_num_per_stock = math.ceil(days * TRADE_DAYS_PER_YEAR / 365)
        stock_num = math.ceil(MAX_RECORDS / record_num_per_stock)
        stock_num = min(stock_num, MAX_STOCKS_BATCH)  # 2022.6.16, 触发过一个批次2400只的情况，太多了，做一个限制
        logger.debug("下载优化:共%d天,每只股票%d条,每次下载4800条，所以，可以一次可下载%d只股票", days, record_num_per_stock, stock_num)
        return stock_num

    def optimized_batch_download(self, func, stock_codes, multistocks, **kwargs):
        """
        使用优化完的参数，来下载股票，一次可以支持1只或多只，由参数multistocks决定。

        支持多只的时候，需要使用函数calculate_best_fetch_stock_num，计算到底一次下载几只最优

        :param func: 调用的tushare的api的函数
        :param multistocks: 是否支持同时取多只股票,原因是pro_bar不支持：https://tushare.pro/document/2?doc_id=109
        :param kwargs:
        :return:
        """
        start_time = time.time()

        if stock_codes is None:
            stock_codes = utils.get_stock_codes(self.db_engine)

        end_date = utils.date2str(datetime.datetime.now())
        if multistocks:
            # 如果是需要多只股票一起下载，去看下库中所有股票的最新日期，粗略估计算是所有的股票的起始日期
            # 但其实，每只股票下载的时候，还会去找自己的真正最后的更新日期
            start_date = self.get_start_date()
            stock_num_once = self.calculate_best_fetch_stock_num(start_date, end_date)

            if stock_num_once ==0:
                logger.info("[多股票同同时下载] 因为已经是截止到今天的数据了，无需下载")
                stock_codes=[]
            else:
                stock_codes = np.array_split(stock_codes, math.ceil(len(stock_codes) / stock_num_once))  # 定义几组
                stock_codes = [",".join(stocks) for stocks in stock_codes]
                logger.debug("支持多股票下载，下载 %s~%s 的%d只股票，粗略估算：共%d个批次，每批次%d只股票同时获取",
                         start_date,
                         end_date,
                         len(stock_codes),
                         len(stock_codes),
                         stock_num_once)

        logger.debug("调用[%s]，共%d个批次，下载间隔%.0f毫秒",
                     self.get_table_name(),
                     len(stock_codes),
                     self.call_interval)
        df_all = []
        pbar = tqdm(total=len(stock_codes))
        for i, ts_code in enumerate(stock_codes):

            start_date = self.get_start_date(where=f"ts_code='{ts_code}'")

            if not self.__need_download(start_date):
                logger.debug("股票[%s]已经是最新数据，无需下载")
                continue

            if start_date > end_date:
                logger.warning("股票[%s] 开始日期%s > 结束日期%s，无需下载", ts_code, start_date, end_date)
                return

            df = self.retry_call(func=func,
                                 ts_code=ts_code,
                                 start_date=start_date,
                                 end_date=end_date,
                                 **kwargs)
            if len(df) == 0:
                logger.warning("股票[%s] %s~%s 下载条数为0", ts_code,start_date,end_date)
                continue
            if i % 100 == 0:
                logger.debug("下载进度：%d/%d", i, len(stock_codes))
            df_all.append(df)
            pbar.update(1)
        pbar.close()

        if len(df_all)>0:
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

    def __need_download(self, start_date):
        if utils.today() == start_date and datetime.datetime.now().time() < datetime.time(TODAY_TIMING, 00):
            logger.info("最后需要更新的日期[%s]是今天，且未到[%d点]，无需下载最新数据", start_date, TODAY_TIMING)
            return False
        return True

    def get_stocks_from_argument(self):
        """这个是为了指定下载少量股票，用于本地调试"""
        parser = argparse.ArgumentParser()
        parser.add_argument('-f', '--file', type=str, default=None, help="下载股票列表的文件")
        parser.add_argument('-c', '--code', type=str, default=None, help="要下载的股票")
        # 防止命令行调用这个，比如在realtime.sh中调用，跟参数不一致的情况
        # 这样命令行有了别的参数传入，只要不是这2个，都忽略
        args, unknown = parser.parse_known_args()
        stocks = []
        if args.file:
            import pandas as pd
            df = pd.read_csv(args.file)
            assert len(df.columns) == 1, "导入文件只能有一列，即股票代码"
            _stocks = df.iloc[:, 0].tolist()
            logger.debug("加载从[%s]读取的%d只股票代码", args.file, len(_stocks))
            stocks += _stocks

        if args.code:
            _stocks = args.code.split(",")
            stocks += _stocks
            logger.debug("加载从参数[code]中读取的%d只股票代码", len(_stocks))

        if len(stocks) == 0: return None

        return stocks
