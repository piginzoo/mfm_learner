import calendar
import datetime
import logging
import os
import time
import warnings

import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas import Series
from sqlalchemy import create_engine

from mfm_learner.utils import CONF
from mfm_learner.utils import utils

logger = logging.getLogger(__name__)

DB_FILE = "../data/tushare.db"


def get_stock_codes(db_engine):
    df = pd.read_sql('select * from stock_basic', db_engine)
    return df['ts_code']


def connect_db():
    uid = CONF['datasources']['mysql']['uid']
    pwd = CONF['datasources']['mysql']['pwd']
    db = CONF['datasources']['mysql']['db']
    host = CONF['datasources']['mysql']['host']
    port = CONF['datasources']['mysql']['port']
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(uid, pwd, host, port, db, 'utf8'))
    # engine = create_engine('sqlite:///' + DB_FILE + '?check_same_thread=False', echo=echo)  # 是否显示SQL：, echo=True)
    return engine


def str2date(s_date, format="%Y%m%d"):
    return datetime.datetime.strptime(s_date, format)


def get_monthly_duration(start_date, end_date):
    """
    把开始日期到结束日期，分割成每月的信息
    比如20210301~20220515 =>
    [   [20210301,20210331],
        [20210401,20210430],
        ...,
        [20220401,20220430],
        [20220501,20220515]
    ]
    """

    start_date = str2date(start_date)
    end_date = str2date(end_date)
    years = list(range(start_date.year, end_date.year + 1))
    scopes = []
    for year in years:
        if start_date.year == year:
            start_month = start_date.month
        else:
            start_month = 1

        if end_date.year == year:
            end_month = end_date.month + 1
        else:
            end_month = 12 + 1

        for month in range(start_month, end_month):

            if start_date.year == year and start_date.month == month:
                s_start_date = date2str(datetime.date(year=year, month=month, day=start_date.day))
            else:
                s_start_date = date2str(datetime.date(year=year, month=month, day=1))

            if end_date.year == year and end_date.month == month:
                s_end_date = date2str(datetime.date(year=year, month=month, day=end_date.day))
            else:
                _, last_day = calendar.monthrange(year, month)
                s_end_date = date2str(datetime.date(year=year, month=month, day=last_day))

            scopes.append([s_start_date, s_end_date])

    return scopes


def get_yearly_duration(start_date, end_date):
    """
    把开始日期到结束日期，分割成每年的信息
    比如20210301~20220501 => [[20210301,20211231],[20220101,20220501]]
    """
    start_date = str2date(start_date)
    end_date = str2date(end_date)
    years = list(range(start_date.year, end_date.year + 1))
    scopes = [[f'{year}0101', f'{year}1231'] for year in years]

    if start_date.year == years[0]:
        scopes[0][0] = date2str(start_date)
    if end_date.year == years[-1]:
        scopes[-1][1] = date2str(end_date)

    return scopes


def tomorrow(s_date=None):
    if s_date is None: s_date = today()
    return future('day', 1, s_date)


def yesterday(s_date=None):
    if s_date is None: s_date = today()
    return last_day(s_date, 1)


def last(date_type, unit, s_date):
    return __date_span(date_type, unit, -1, s_date)


def last_year(s_date, num=1):
    return last('year', num, s_date)


def last_month(s_date, num=1):
    return last('month', num, s_date)


def last_week(s_date, num=1):
    return last('week', num, s_date)


def last_day(s_date, num=1):
    return last('day', num, s_date)


def today():
    now = datetime.datetime.now()
    return datetime.datetime.strftime(now, "%Y%m%d")


def future(date_type, unit, s_date):
    return __date_span(date_type, unit, 1, s_date)


def __date_span(date_type, unit, direction, s_date):
    """
    last('year',1,'2020.1.3')=> '2019.1.3'
    :param unit:
    :param date_type: year|month|day
    :return:
    """
    the_date = str2date(s_date)
    if date_type == 'year':
        return date2str(the_date + relativedelta(years=unit) * direction)
    elif date_type == 'month':
        return date2str(the_date + relativedelta(months=unit) * direction)
    elif date_type == 'week':
        return date2str(the_date + relativedelta(weeks=unit) * direction)
    elif date_type == 'day':
        return date2str(the_date + relativedelta(days=unit) * direction)
    else:
        raise ValueError(f"无法识别的date_type:{date_type}")


def date2str(date, format="%Y%m%d"):
    return datetime.datetime.strftime(date, format)


def dataframe2series(df):
    if type(df) == Series: return df
    assert len(df.columns) == 1, df.columns
    return df.iloc[:, 0]


def init_logger(file=True):
    print("开始初始化日志：file=%r" % (file))

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger('matplotlib.font_manager').disabled = True
    logging.getLogger('matplotlib.colorbar').disabled = True
    logging.getLogger('matplotlib').disabled = True
    logging.getLogger('fontTools.ttLib.ttFont').disabled = True
    warnings.filterwarnings("ignore")
    warnings.filterwarnings("ignore", module="matplotlib")

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d P%(process)d: %(message)s')

    root_logger = logging.getLogger()
    root_logger.setLevel(level=logging.DEBUG)

    def is_any_handler(handlers, cls):
        for t in handlers:
            if type(t) == cls: return True
        return False

    # 加入控制台
    if not is_any_handler(root_logger.handlers, logging.StreamHandler):
        stream_handler = logging.StreamHandler()
        root_logger.addHandler(stream_handler)
        print("日志：创建控制台处理器")

    # 加入日志文件
    if file and not is_any_handler(root_logger.handlers, logging.FileHandler):
        if not os.path.exists("./logs"): os.makedirs("./logs")
        filename = "./logs/{}.log".format(time.strftime('%Y%m%d%H%M', time.localtime(time.time())))
        t_handler = logging.FileHandler(filename)
        root_logger.addHandler(t_handler)
        print("日志：创建文件处理器", filename)

    handlers = root_logger.handlers
    for handler in handlers:
        handler.setLevel(level=logging.DEBUG)
        handler.setFormatter(formatter)


def __calc_OHLC_in_group(df_in_group):
    """
    计算一个分组内的最大的、最小的、开盘、收盘 4个值
    """
    # 先复制最后一条（即周五或者月末日），为了得到所有的字段
    df_result = df_in_group.tail(1).copy()
    df_result['open'] = df_in_group.loc[df_in_group.index.min()]['open']
    df_result['close'] = df_in_group.loc[df_in_group.index.max()]['close']
    df_result['high'] = df_in_group['high'].max()
    df_result['low'] = df_in_group['low'].min()
    df_result['volume'] = df_in_group['volume'].sum()
    df_result['amount'] = df_in_group['amount'].sum()
    return df_result


def day2month(df):
    """
    返回，数据中，每个月，最后一天的数据
    """
    # 按照日期索引，进行分组
    df_result = df.groupby(df.index.to_period('M')).apply(__calc_OHLC_in_group)
    if len(df_result.index.names)>1:
        df_result = df_result.droplevel(level=0)
    df_result['pct_chg'] = df_result.close.pct_change()
    return df_result


def day2week(df):
    """
    返回，数据中，每周，最后一天的数据

    使用分组groupby返回的结果中多出一列，所以要用dropLevel 来drop掉
                                           code      open      high       low  ...   change   pct_chg      volume       amount
    datetime              datetime                                             ...
    2007-12-31/2008-01-06 2008-01-04  000636.SZ  201.0078  224.9373  201.0078  ...  -1.4360       NaN   352571.00   479689.500
    2008-01-07/2008-01-13 2008-01-11  000636.SZ  217.7585  223.1825  201.0078  ...  -6.5400 -0.027086   803621.33  1067058.340
    """
    # to_period是转成
    df_result = df.groupby(df.index.to_period('W')).apply(__calc_OHLC_in_group)
    if len(df_result.index.names)>1:
        df_result = df_result.droplevel(level=0) # 多出一列datetime，所以要drop掉
    df_result['pct_chg'] = df_result.close.pct_change()
    return df_result


def get_trade_period(the_date, period, datasource):
    """
    返回某一天所在的周、月的交易日历中的开始和结束日期
    比如，我传入是 2022.2.15， 返回是的2022.2.2/2022.2.27（这2日是2月的开始和结束交易日）
    datasource是传入的
    the_date：格式是YYYYMMDD
    period：W 或 M
    """

    the_date = utils.str2date(the_date)

    # 读取交易日期
    df = datasource.trade_cal(exchange='SSE', start_date=today, end_date='20990101')
    # 只保存日期列
    df = pd.DataFrame(df, columns=['cal_date'])
    # 转成日期型
    df['cal_date'] = pd.to_datetime(df['cal_date'], format="%Y%m%d")
    # 如果今天不是交易日，就不需要生成
    if pd.Timestamp(the_date) not in df['cal_date'].unique(): return False

    # 把日期列设成index（因为index才可以用to_period函数）
    df = df[['cal_date']].set_index('cal_date')
    # 按照周、月的分组，对index进行分组
    df_group = df.groupby(df.index.to_period(period))
    # 看传入日期，是否是所在组的，最后一天，即，周最后一天，或者，月最后一天
    target_period = None
    for period, dates in df_group:
        if period.start_time < pd.Timestamp(the_date) < period.end_time:
            target_period = period
    if target_period is None:
        logger.warning("无法找到上个[%s]的开始、结束日期", period)
        return None, None
    return period[0], period[-1]


def get_last_trade_date(end_date, trade_dates):
    """
    得到日期范围内的最后的交易日，end_date可能不在交易日里，所以要找一个最近的日子
    :param df_trade_date: 所有交易日
    :return: 只保留每个月的最后一个交易日，其他剔除掉
    """
    # 反向排序
    trade_dates = trade_dates.tolist()
    trade_dates.reverse()

    # 寻找合适的交易日期
    for trade_date in trade_dates:

        # 从最后一天开始找，如果交易日期(trade_date)比目标日期(end_date)小了，就找到了
        if trade_date < end_date:
            return trade_date
    return None
