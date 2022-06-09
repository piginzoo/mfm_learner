import datetime
from datetime import date

import pandas as pd
import os
import time

from mfm_learner.utils import utils
import tushare

pro = tushare.pro_api()
TUSHARE_DATA_DIR="data/tushare_data/data"

def daily(fuquan="hfq"):
    file_path = os.path.join(TUSHARE_DATA_DIR,
                             "daily",
                             "daily_{}.csv".format(fuquan))
    dtype_dic = {'ts_code': str, 'trade_date': str,'ann_date': str}
    df = pd.read_csv(file_path,dtype=dtype_dic)
    # df = pd.read_csv(file_path)
    return df


def test_trade_cal():
    today = datetime.date.today()
    df = pro.trade_cal(exchange='SSE', start_date=utils.date2str(date.today()), is_open=1)
    df['cal_date'] = pd.to_datetime(df['cal_date'],format="%Y%m%d")
    if pd.Timestamp(today) not in df['cal_date'].unique(): return False
    df = df[['cal_date']].set_index('cal_date')
    df_group = df.groupby(df.index.to_period('W'))
    now = datetime.date.today()
    for period,dates in df_group:
        if period.start_time < pd.Timestamp(today) < period.end_time:
            return dates.index[-1] == pd.Timestamp(now)
    return False


# python -m test.toy.test_tushare_data
if __name__ == '__main__':
    start_time = time.time()
    # df = daily()
    # print("从文件加载",str(time.time()-start_time), " s")
    # print(df.info())
    # print(df.head(2))

    # start_time = time.time()
    #
    # db_engine = utils.connect_db()
    # res = df.to_sql('daily', db_engine, index=False, if_exists='fail', chunksize=5000)
    #
    # print("保存到数据库中", str(time.time()-start_time), " s")
    # start_time = time.time()

    # db_engine = utils.connect_db()
    # df = pd.read_sql('select * from daily', db_engine)
    # print("从数据库中加载",str(time.time()-start_time), " s")
    # start_time = time.time()

    # db_engine = utils.connect_db()
    # df = pd.read_sql('select * from daily where ts_code="000001.SZ"', db_engine)

    # print("从数据库中加载", str(time.time() - start_time), " s")
    # print(df.head(1))
    # start_time = time.time()

    print(test_trade_cal())
