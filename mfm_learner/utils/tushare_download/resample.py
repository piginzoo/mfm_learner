"""
依据现有的daily数据，生成周和月的数据
"""
import argparse
import datetime
import logging
import pandas as pd
import sqlalchemy
from tqdm import tqdm
from mfm_learner.datasource import datasource_factory, datasource_utils
from mfm_learner.utils import utils, db_utils

logger = logging.getLogger(__name__)
datasource = datasource_factory.get()

"""
2008~2022，14.5年，754条周数据，174条月数据
"""


def main(code=None,force=False):
    db_engine = utils.connect_db()

    if code is None:
        stock_codes = utils.get_stock_codes(db_engine)
    else:
        stock_codes = [code]

    if force:
        is_weekly = is_monthly = True
    else:
        is_weekly = precheck("W")
        is_monthly = precheck("M")

    if not is_weekly and not is_monthly:
        logger.info("今天 %s 不是周最后交易日和月最后交易日，无需生成数据", utils.date2str(datetime.date.today()))
        return

    pbar = tqdm(total=len(stock_codes))
    for stock_code in stock_codes:
        df = datasource.daily(stock_code=stock_code)
        df = datasource_utils.reset_index(df, date_only=True, date_format="%Y%m%d")
        df = df.sort_index(ascending=True)

        if len(df) == 0:
            logger.error("股票[%s]数据不存在，无法进行采样，请尽快下载其数据！", stock_code)
            continue

        if is_weekly: process(db_engine, stock_code, df, "weekly")
        if is_monthly: process(db_engine, stock_code, df, "monthly")
        pbar.update(1)


def precheck(period):
    """
    1. 最松：如果今天，不是交易日的月末，或者，周的最后一天就不运行，需要考虑节假日导致周五或者月末休市，所以要参考交易日期
    TODO: 2. 查看数据库中的每只股票的最后日期，如果这个日期不是上周、上月末的日期，那么，就需要重新生成，
    :return:
    """
    today = datetime.date.today()
    df = datasource.trade_cal(exchange='SSE', start_date=today, end_date='20990101')
    df = pd.DataFrame(df,columns=['cal_date'])
    df['cal_date'] = pd.to_datetime(df['cal_date'], format="%Y%m%d")
    if pd.Timestamp(today) not in df['cal_date'].unique(): return False

    df = df[['cal_date']].set_index('cal_date')
    df_group = df.groupby(df.index.to_period(period))
    now = datetime.date.today()
    for period, dates in df_group:
        if period.start_time < pd.Timestamp(today) < period.end_time:
            return dates.index[-1] == pd.Timestamp(now)
    return False


def delete_stale(engine, code, table_name):
    delete_sql = f"""
        delete from {table_name} 
        where 
            ts_code='{code}'
    """
    if not db_utils.is_table_exist(engine, table_name): return
    # logger.debug("从%s中删除%s的旧数据", table_name, code)
    db_utils.run_sql(engine, delete_sql)


def process(db_engine, code, df_daily, period):
    # 做一个数据采样：月或者周
    if period == "weekly":
        df = utils.day2week(df_daily)
    elif period == "monthly":
        df = utils.day2month(df_daily)
    else:
        raise ValueError(period)

    # 要把之前统一为backtrader的格式，还原会tushare本身的命名
    df = df.reset_index()
    df = df.rename(columns={'vol': 'volume', 'code': 'ts_code', 'datetime': 'trade_date'})  # 列名改回去，尊崇daliy_hdf的列名
    df['trade_date'] = df['trade_date'].apply(utils.date2str)
    dtype_dic = {
        'ts_code': sqlalchemy.types.VARCHAR(length=9),
        'trade_date': sqlalchemy.types.VARCHAR(length=8),
    }

    # 先把这只股票的旧数据删除掉
    table_name = f"{period}_hfq"
    delete_stale(db_engine, code, table_name)

    # 重新保存新的数据
    df.to_sql(table_name,
              db_engine,
              index=False,
              if_exists="append",
              dtype=dtype_dic,
              chunksize=1000)
    logger.debug("导入股票 [%s] %s周期 %d条数据=>db[表%s] ", code, period, len(df), table_name)


"""
python -m mfm_learner.utils.tushare_download.resample -c 603233.SH
python -m mfm_learner.utils.tushare_download.resample -c 603233.SH -f
"""
if __name__ == '__main__':
    utils.init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--code', type=str, default=None)
    parser.add_argument('-f', '--force', action='store_true', default=False, help="是否强制")
    args = parser.parse_args()

    main(args.code,args.force)
