import logging
import math
import os.path
import time

import sqlalchemy
import tushare
import pandas as pd
from utils import utils, CONF
from utils.tushare_download.download_utils import is_table_exist

logger = logging.getLogger(__name__)

RETRY = 5  # 尝试几次
WAIT = 1  # 每次delay时常(秒)
CALL_INTERVAL = 0.15 # 150毫秒,1分钟400次
EALIEST_DATE = '20080101' # 最早的数据日期

class BaseDownload():

    def __init__(self):
        self.db_engine = utils.connect_db()
        token = CONF['datasources']['tushare']['token']
        self.pro = tushare.pro_api(token)
        logger.debug("注册到Tushare上，token:%s***", token[:10])
        self.retry_count = 0
        self.save_dir = "data/tushare_download"
        if not os.path.exists(self.save_dir): os.makedirs(self.save_dir)
        self.call_interval = CALL_INTERVAL

    def get_table_name(self):
        raise NotImplemented()

    def get_date_column_name(self):
        raise NotImplemented()

    def get_start_date(self):

        if not is_table_exist(self.db_engine, self.get_table_name()):
            logger.debug("表[%s]在数据库中不存在，返回默认最早开始日期[%s]", self.get_table_name(),EALIEST_DATE)
            return EALIEST_DATE

        table_name = self.get_table_name()
        date_column_name = self.get_date_column_name()
        df = pd.read_sql('select max({}) from {}'.format(date_column_name, table_name), self.db_engine)
        assert len(df) == 1
        latest_date = df.iloc[:, 0].item()
        if latest_date is None:
            logger.debug("表[%s]中无数据，返回默认最早开始日期[%s]", self.get_table_name(), EALIEST_DATE)
            return EALIEST_DATE

        # 日期要往后错一天，比DB中的
        latest_date = utils.tomorrow(latest_date)
        logger.debug("数据库中表[%s]的最后日期[%s]为：%s", table_name, date_column_name, latest_date)
        return latest_date

    def to_db(self, df, table_name, if_exists='append'):
        start_time = time.time()
        dtype_dic = {
            'ts_code': sqlalchemy.types.VARCHAR(length=9),
            'trade_date': sqlalchemy.types.VARCHAR(length=8),
            'ann_date': sqlalchemy.types.VARCHAR(length=8)
        }
        df.to_sql(table_name, self.db_engine, index=False, if_exists=if_exists, dtype=dtype_dic, chunksize=1000)
        logger.debug("导入 [%.2f] 秒, df[%d条]=>db[表%s] ", time.time() - start_time, len(df), table_name)

    def retry_call(self, func, **kwargs):
        """
        Tushare Exception: 抱歉，您每分钟最多访问该接口400次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108
        每200毫秒调用一次，比较安全，大概是一分钟最多是5*60=300次
        """

        while self.retry_count < RETRY:
            try:
                # print(kwargs)
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
        file_path = os.path.join(self.save_dir,name)
        df.to_csv(file_path)
        return file_path
