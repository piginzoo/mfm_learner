import datetime
import logging
import math
import time

import pandas as pd
import numpy as np
import utils.utils
from utils import utils
from utils.tushare_download.base_downloader import BaseDownload
from tqdm import tqdm

logger = logging.getLogger(__name__)

fields = 'ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv'


class DailyBasic(BaseDownload):

    def download(self):
        start_time = time.time()

        start_date = self.get_start_date()
        end_date = utils.date2str(datetime.datetime.now())
        stock_codes = self.get_stock_codes()

        logger.debug("准备下载 %s~%s, %d 只股票的基本信息", start_date, end_date, len(stock_codes))

        # TODO: 没测试呢
        stock_num_once = self.calculate_best_fetch_stock_num(start_date,end_date)
        stock_codes_array = np.split(stock_codes, math.ceil(len(stock_codes)/stock_num_once))
        stock_codes_array = [",".join(stocks) for stocks in stock_codes_array]

        df_daily_basic = []
        pbar = tqdm(total=len(stock_codes_array))
        for i, ts_code in enumerate(stock_codes_array):
            df = self.retry_call(func=self.pro.daily_basic,
                                 ts_code=ts_code,
                                 start_date=start_date,
                                 end_date=end_date,
                                 fields=fields)
            df_daily_basic.append(df)
            # Tushare Exception: 抱歉，您每分钟最多访问该接口400次，
            # 权限的具体详情访问：https://tushare.pro/document/1?doc_id=108
            time.sleep(self.call_interval)
            pbar.update(i)
        pbar.close()

        df_daily_basic = pd.concat(df_daily_basic)

        self.save(df=df_daily_basic, name="df_daily_basic_{}_{}.csv".format(start_date, end_date))

        logger.debug("下载了 %s~%s, %d只股票的%d条数据, %.2f秒",
                     start_date,
                     end_date,
                     len(stock_codes),
                     len(df_daily_basic),
                     time.time() - start_time)
        # self.to_db(df_new, "stock_basic")

    def get_start_date(self):
        df = pd.read_sql('select max(trade_date) from daily_basic', self.db_engine)
        assert len(df) == 1
        latest_date = df.iloc[:, 0].item()
        logger.debug("数据库中daily_basic的最后日期为：%s", latest_date)
        return latest_date

    def get_stock_codes(self):
        df = pd.read_sql('select * from stock_basic', self.db_engine)
        return df['ts_code']


# python -m utils.tushare_download.daily_basic
if __name__ == '__main__':
    utils.init_logger()
    downloader = DailyBasic()
    downloader.download()
