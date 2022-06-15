"""
依据现有的daily数据，生成周和月的数据
"""
import argparse
import datetime
import logging
import time

import pandas as pd
import sqlalchemy
from tqdm import tqdm

from mfm_learner.datasource import datasource_factory, datasource_utils
from mfm_learner.utils import utils, db_utils, multi_processor

logger = logging.getLogger(__name__)
datasource = datasource_factory.get()

"""
2008~2022，14.5年，754条周数据，174条月数据

判断逻辑：
    - 挨个股票来查，查每支股票的 "最后日期"
    - 如果 "最后日期" 在 "当前日期" 的上一个周、月的交易日历范围内，或者，和当前日期一样，就不需要生成了
    - 如果 "最后日期" 不在 "当前日期" 的上一个周、月的交易日历里，就需要生成，
      "上一个"很重要，因为当前周期（月、周）尚未结束，那么触发拉取最新的日数据，生成月、周数据
生成逻辑：
    直接用[最后日期+1天]~[当前日期]的日数据，生成，不用考虑重复，因为之前的已经过滤了
"""


def main(code=None, num=None, worker=None):
    start = time.time()
    db_engine = utils.connect_db()
    if code is None:
        stock_codes = utils.get_stock_codes(db_engine)
        if num: stock_codes = stock_codes[:num]
    else:
        stock_codes = [code]
    if worker:
        logger.debug("使用多进程进行处理：%d 个进程", worker)
        multi_processor.execute(stock_codes, worker, run)
    else:
        logger.debug("使用单进程进行处理")
        run(stock_codes)

    logger.debug("共耗时: %s ", str(datetime.timedelta(seconds=time.time() - start)))


def __period_mapping(period):
    if "weekly": return "W"
    if "monthly": return "M"


def run(stocks):
    utils.init_logger()
    db_engine = utils.connect_db()

    pbar = tqdm(total=len(stocks))
    df_trade_date_group_by_week = __group_trade_dates_by(__period_mapping('weekly'))  # 交易日期按照周分组
    for stock_code in stocks:
        process(db_engine, stock_code, df_trade_date_group_by_week, "weekly")
        pbar.update(1)

    pbar = tqdm(total=len(stocks))
    df_trade_date_group_by_month = __group_trade_dates_by(__period_mapping('monthly'))  # 交易日期按照月分组
    for stock_code in stocks:
        process(db_engine, stock_code, df_trade_date_group_by_month, "monthly")
        pbar.update(1)


def __group_trade_dates_by(period):
    """按照分组"""

    # 读取交易日期
    df = datasource.trade_cal(exchange='SSE', start_date=db_utils.EALIEST_DATE, end_date=utils.today())
    # 只保存日期列
    df = pd.DataFrame(df, columns=['cal_date'])
    # 转成日期型
    df['cal_date'] = pd.to_datetime(df['cal_date'], format="%Y%m%d")
    # 把日期列设成index（因为index才可以用to_period函数）
    df = df[['cal_date']].set_index('cal_date')
    # 按照周、月的分组，对index进行分组
    df_group = df.groupby(df.index.to_period(period))
    return df_group


def __need_resample(code, period, stock_period_latest_date, df_trade_groups):
    """
    如果 "最后日期" 在 "当前日期" 的上一个周、月的交易日历范围内，或者，和当前日期一样，就不需要生成了
    latest_date： 最后日期
    stock_period_latest_date： 股票的周、月的最后一天
    df_trade_groups：按照周、月对交易日历进行了分组
    """
    # 当前日期，和，股票的最后周|月采样日期一样，那说明已经采样过了
    today = datetime.date.today()
    if stock_period_latest_date == today:
        logger.debug("股票[%s]的周期[%s]的最后日期[%s]，和今天[%s]一样，无需再采样了", code, period, stock_period_latest_date, today)
        return False

    # 得到今天对应的上周、上月的那个周期last_period
    if period == 'weekly':
        last_period_date = utils.last_week(utils.today())
    elif period == 'monthly':
        last_period_date = utils.last_month(utils.today())
    else:
        raise ValueError(period)
    last_period = None
    for p, dates in df_trade_groups:
        if p.start_time < pd.Timestamp(last_period_date) < p.end_time:
            logger.debug("今天[%s],对应的上%s[%s],对应的周期：%s~%s", utils.today(),
                         period,
                         last_period_date,
                         utils.date2str(p.start_time),
                         utils.date2str(p.end_time))
            last_period = p
    assert last_period, '上周的交易周期不可能为空'

    # 看股票的最后日期，是不是在，今天对应的上周、上月的那个周期last_period的范围里
    # 如果在，说明已经生成了，否则，就说明没生成过，需要生成
    is_between_last_period = last_period.start_time < pd.Timestamp(
        utils.str2date(stock_period_latest_date)) < last_period.end_time
    if not is_between_last_period: logger.debug("股票[%s]的%s的最后日期%s，不在%s~%s范围内",
                                                code,
                                                period,
                                                stock_period_latest_date,
                                                utils.date2str(p.start_time),
                                                utils.date2str(p.end_time))

    return not is_between_last_period


def delete_stale(engine, code, table_name):
    delete_sql = f"""
        delete from {table_name} 
        where 
            ts_code='{code}'
    """
    if not db_utils.is_table_exist(engine, table_name): return
    # logger.debug("从%s中删除%s的旧数据", table_name, code)
    db_utils.run_sql(engine, delete_sql)


def get_table_name(period):
    return f"{period}_hfq"


def process(db_engine, code, df_trade_date_group, period):
    stock_period_latest_date = db_utils.get_start_date(get_table_name(period),
                                                       'trade_date',
                                                       db_engine,
                                                       where=f'ts_code="{code}"')

    # 如果这个股票不需要采样，返回
    if not __need_resample(code, period, stock_period_latest_date, df_trade_date_group):
        return

    logger.debug("需要对股票[%s]进行采样: %s~%s",code,stock_period_latest_date,utils.today())

    df = datasource.daily(stock_code=code, start_date=stock_period_latest_date, end_date=utils.today())
    df = datasource_utils.reset_index(df, date_only=True, date_format="%Y%m%d")
    df = df.sort_index(ascending=True)
    if len(df) == 0:
        logger.warning("股票[%s] %s~%s，没有数据！", code, stock_period_latest_date, utils.today())
        return

    # 做一个数据采样：月或者周
    if period == "weekly":
        df = utils.day2week(df)
    elif period == "monthly":
        df = utils.day2month(df)
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

    # 重新保存新的数据
    df.to_sql(get_table_name(period),
              db_engine,
              index=False,
              if_exists="append",
              dtype=dtype_dic,
              chunksize=1000)
    logger.debug("导入股票 [%s] %s周期 %d条数据=>db[表%s] ", code, period, len(df), get_table_name(period))

    # 保存到数据库中的时候，看看有无索引，如果没有，创建之
    db_utils.create_db_index(db_engine, get_table_name(period), df)


"""
python -m mfm_learner.utils.tushare_download.resample -c 603233.SH
python -m mfm_learner.utils.tushare_download.resample -c 603233.SH -f
python -m mfm_learner.utils.tushare_download.resample -w 3 -n 200 -f
"""
if __name__ == '__main__':
    utils.init_logger()
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--code', type=str, default=None)
    parser.add_argument('-n', '--num', type=int, default=1000000000)
    parser.add_argument('-w', '--worker', type=int, default=None)
    args = parser.parse_args()

    main(args.code, args.num, args.worker)
