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
问题：
    有一个问题，就是我按照周期对日期做分组，然后每个分组内统计OHLC，但是最后一个分组有问题，
    如果最后一个分组的最后一天不是周五或者月末，其实，这条记录应该不需要生成的，
    如果生成，会导致最后一条，只有一个半吊子数据，
    比如月数据：今天是2.8号，本来不应该生成月数据，但是，目前是生成了一个2月数据（2.1~2.8），这个是不对的。
    解决办法是，在使用日数据（到2.8号的数据）的时候，如果不是月末最后一天，就只生成到上个月，如果是最后一天，就生成本月。
"""


def main(code=None, end_date=None, num=None, worker=None):
    start = time.time()
    db_engine = utils.connect_db()
    if code is None:
        stock_codes = utils.get_stock_codes(db_engine)
        if num: stock_codes = stock_codes[:num]
    else:
        stock_codes = [code]
    if worker:
        logger.debug("使用多进程进行处理：%d 个进程", worker)
        multi_processor.execute(stock_codes, worker, run, end_date=end_date)
    else:
        logger.debug("使用单进程进行处理")
        run(stock_codes, end_date)

    logger.debug("共耗时: %s ", str(datetime.timedelta(seconds=time.time() - start)))


def __period_mapping(period):
    """根据weekly=>W，W是按照index分组的时候用"""
    if period == "weekly": return "W"
    if period == "monthly": return "M"
    raise ValueError(period)


def run(stocks, end_date):
    utils.init_logger()
    db_engine = utils.connect_db()  # 重新获得db_engine，多进程不能共享db连接
    run_by_period(stocks, 'weekly', end_date, db_engine)
    run_by_period(stocks, 'monthly', end_date, db_engine)


def run_by_period(stocks, s_period, end_date, db_engine):
    """
    s_periond: weekly|monthly
    :param stocks:
    :param s_period:
    :param end_date:
    :param db_engine:
    :return:
    """
    # 找到今天，对应的合适的时间期间

    pbar = tqdm(total=len(stocks))
    df_all = []

    # 读取交易日期
    df_calendar = datasource.trade_cal(exchange='SSE', start_date=db_utils.EALIEST_DATE, end_date=end_date)
    # 安州周期(weekly|monthly)得到交易日期的分组
    df_trade_date_group = __group_trade_dates_by(df_calendar, __period_mapping(s_period))  # 交易日期按照周分组
    # 按照s_period的类型，找到包含end_date的周期（月|周）
    target_period = get_last_period(s_period, end_date, df_trade_date_group, df_calendar)
    logger.debug("包含日期[%s]的[%s]周期为：%r", end_date, s_period, target_period)
    # 处理每只股票
    for stock_code in stocks:
        df = process(db_engine, stock_code, target_period, s_period)
        if df is not None: df_all.append(df)
        pbar.update(1)
    if len(df_all) > 0:
        df_all = pd.concat(df_all)
        save_db(df_all, s_period, db_engine)


def __group_trade_dates_by(df_calendar, period):
    """按照分组"""

    # 只保存日期列
    df = pd.DataFrame(df_calendar, columns=['cal_date'])
    # 转成日期型
    df['cal_date'] = pd.to_datetime(df['cal_date'], format="%Y%m%d")
    # 把日期列设成index（因为index才可以用to_period函数）
    df = df[['cal_date']].set_index('cal_date')
    # 按照周、月的分组，对index进行分组
    df_group = df.groupby(df.index.to_period(period))
    return df_group


def get_last_period(s_period, end_date, df_trade_groups, df_calendar):
    """
    按照今天的日子，寻找，最后的周期(最后一周、最后一个月）
    如果今天是周期的最后一天，返回包含今天的周期，
    否则，返回上个周期
    s_period：weekly|monthly
    df_trade_groups：已经按weekly或者monthly周期，分组好的dataframe交易数据
    """

    # 看今天是不是周期的最后一天（从交易数据的日期索引中获得）
    this_period = find_period_contain_the_day(s_period, end_date, df_trade_groups, 'this')

    # 20220703 bugfix，piginzoo，需要用交易日来找周期，否则，容易找到上个周期去
    # 比如end_date是7.2（周六），那么，返回的weekly周期应该是6.27~7.3。
    nearest_trade_day_of_end_date = utils.get_last_trade_date(end_date, df_calendar)
    nearest_trade_day_of_period_end = utils.get_last_trade_date(utils.date2str(this_period.end_time), df_calendar)

    if nearest_trade_day_of_end_date == nearest_trade_day_of_period_end:
        logger.debug("目标日[%s]对应的交易日[%s]是%s周期最后一个交易日，采样周期为[%s~%s]",
                     end_date,
                     nearest_trade_day_of_end_date,
                     s_period,
                     utils.date2str(this_period.start_time),
                     utils.date2str(this_period.end_time))
        return this_period

    # 看最后的日期，是不是在
    last_period = find_period_contain_the_day(s_period, end_date, df_trade_groups, 'last')
    logger.debug("目标日[%s]不是%s最后一个交易日，采样周期确定为上周期[%s~%s]",
                 end_date,
                 s_period,
                 utils.date2str(last_period.start_time),
                 utils.date2str(last_period.end_time))
    return last_period


def find_period_contain_the_day(period, end_date, df_trade_groups, this_or_last):
    """
    用来得到当前日期，对应的交易日期周期（period，是一个期间）；或者上一个周期（由this_or_last=='last')
    :param df_trade_groups: 交易日历表，已经按照周期分了组
    :param period: weekly 还是 monthly
    :param this_or_last: this 还是 last
    :return:
    """

    if this_or_last == 'this':
        the_date = end_date
    elif this_or_last == 'last':
        if period == 'weekly':
            the_date = utils.last_week(end_date)
        elif period == 'monthly':
            the_date = utils.last_month(end_date)
        else:
            raise ValueError(period)
    else:
        raise ValueError(this_or_last)

    # 把交易日期，按照周期分组了，每周期（周、月）的在一组
    # 看索引中包含目标日期（the_date)的那组（那组是一个period对象）
    last_period = None
    for p, dates in df_trade_groups:
        if p.start_time < pd.Timestamp(the_date) < p.end_time:
            # 找到包含指定日期的一组
            last_period = p
    assert last_period, '查找的交易周期不可能为空'
    return last_period


# def delete_stale(engine, code, table_name):
#     delete_sql = f"""
#         delete from {table_name}
#         where
#             ts_code='{code}'
#     """
#     if not db_utils.is_table_exist(engine, table_name): return
#     # logger.debug("从%s中删除%s的旧数据", table_name, code)
#     db_utils.run_sql(engine, delete_sql)


def get_table_name(period):
    return f"{period}_hfq"


def process(db_engine, code, target_period, s_period):
    # 得到这只股票，在采样表（weekly_hfq,monthly_hfq）中的，最新的数据日期
    stock_period_latest_date = db_utils.get_last_date(get_table_name(s_period),
                                                      'trade_date',
                                                      db_engine,
                                                      where=f'ts_code="{code}"')

    # 看这只股票的这个周期的最后的日期，是不是在目标周期target_period，里面
    # 在的话，说明这个周期的数据已经采样过了，无需再采样了；否则，采
    is_between_the_period = target_period.start_time <= \
                            pd.Timestamp(utils.str2date(stock_period_latest_date)) \
                            <= target_period.end_time
    if is_between_the_period:
        logger.debug("股票[%s]%s周期最后日期为[%s]，表明%s~%s已采样过，无需再采样",
                     code,
                     s_period,
                     stock_period_latest_date,
                     utils.date2str(target_period.start_time),
                     utils.date2str(target_period.end_time))
        return None

    # 按照开始日期=数据库中股票的最后一天，结束日期=确定的采样周期的最后一天，去检索日频数据
    start_date = utils.tomorrow(stock_period_latest_date)  # 数据库里最后的日子
    end_date = utils.date2str(target_period.end_time)  # 周期的结束的日子
    if start_date > end_date:
        logger.warning("股票[%s]在库中最新的日期，比目标周期[%r]的结束日还新，[%s]数据无需生成",
                       code, target_period, s_period)
        return None

    logger.debug("需要对股票[%s]进行%s周期采样: %s~%s", code, s_period, start_date, end_date)
    df = datasource.daily(stock_code=code, start_date=start_date, end_date=end_date)
    df = datasource_utils.reset_index(df, date_only=True, date_format="%Y%m%d")
    df = df.sort_index(ascending=True)
    if len(df) == 0:
        logger.warning("股票[%s] %s~%s，没有数据！", code, start_date, end_date)
        return None

    # 做一个数据采样：月或者周
    if s_period == "weekly":
        df_sampled = utils.day2week(df)
    elif s_period == "monthly":
        df_sampled = utils.day2month(df)
    else:
        raise ValueError(s_period)

    return df_sampled


def save_db(df, period, db_engine):
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
    logger.debug("保存%d条%s周期采样数据=>db[表%s] ", len(df), period, get_table_name(period))

    # 保存到数据库中的时候，看看有无索引，如果没有，创建之
    db_utils.create_db_index(db_engine, get_table_name(period), df)


"""
python -m mfm_learner.utils.tushare_download.resample -c 300137.SZ -e 20220702
python -m mfm_learner.utils.tushare_download.resample -c 603233.SH -f
python -m mfm_learner.utils.tushare_download.resample -w 3 -n 200 -f
"""
if __name__ == '__main__':
    utils.init_logger()
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--code', type=str, default=None)
    parser.add_argument('-e', '--end_date', type=str, default=utils.today(), help="实盘测试目标日（不一定是今天）")
    parser.add_argument('-n', '--num', type=int, default=1000000000)
    parser.add_argument('-w', '--worker', type=int, default=None)
    args = parser.parse_args()

    main(args.code, args.end_date, args.num, args.worker)
