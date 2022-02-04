import logging
import math
import os.path
import time

import sqlalchemy
import tushare

from utils import utils, CONF

logger = logging.getLogger(__name__)

RETRY = 5  # 尝试几次
WAIT = 1  # 每次delay时常(秒)
CALL_INTERVAL = 0.15 # 150毫秒,1分钟400次
TRADE_DAYS_PER_YEAR = 252 # 1年的交易日
MAX_RECORDS = 4800 # 最多一次的下载行数，tushare是5000，稍微降一下到4800

class BaseDownload():

    def __init__(self):
        self.db_engine = utils.connect_db()
        token = CONF['datasources']['tushare']['token']
        self.pro = tushare.pro_api(token)
        self.retry_count = 0
        self.save_dir = "data/tushare_download"
        if not os.path.exists(self.save_dir): os.makedirs(self.save_dir)
        self.call_interval = CALL_INTERVAL

    def calculate_best_fetch_stock_num(self,start_date,end_date):

        """计算最多可以下载多少只股票"""
        delta = utils.date2str(start_date) - utils.date2str(end_date)
        days = delta.days
        record_num_per_stock = math.floor(days * TRADE_DAYS_PER_YEAR/365)
        return math.floor(MAX_RECORDS/record_num_per_stock)


    def to_db(self, df, table_name):
        start_time = time.time()
        dtype_dic = {
            'ts_code': sqlalchemy.types.VARCHAR(length=9),
            'trade_date': sqlalchemy.types.VARCHAR(length=8),
            'ann_date': sqlalchemy.types.VARCHAR(length=8)
        }
        df.to_sql(table_name, self.db_engine, index=False, if_exists='append', dtype=dtype_dic, chunksize=1000)
        logger.debug("导入 [%.2f] 秒, df[%d条]=>db[表%s] ", time.time() - start_time, len(df), table_name)

    def retry_call(self, func, **kwargs):
        """
        Tushare Exception: 抱歉，您每分钟最多访问该接口400次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108
        每200毫秒调用一次，比较安全，大概是一分钟最多是5*60=300次
        """

        while self.retry_count < RETRY:
            try:
                df = func(**kwargs)
                return df
            except:
                logger.exception("调用Tushare函数[%s]失败:%r", str(func), kwargs)
                sleep = int(math.pow(2, self.retry_count))
                logger.debug("sleep %d 秒再试", sleep)
                time.sleep(sleep)
                self.retry_count += 1
        raise RuntimeError("尝试调用Tushare API多次失败......")

    def save(self, name, df):
        df.to_csv(os.path.join(self.save_dir,name))
