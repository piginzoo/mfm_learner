import logging
import time

import sqlalchemy
import pandas as pd
from mfm_learner.utils import utils

EALIEST_DATE = '20080101'  # 最早的数据日期
logger = logging.getLogger(__name__)


def is_table_exist(engine, name):
    return sqlalchemy.inspect(engine).has_table(name)


def is_table_index_exist(engine, name):
    if not is_table_exist(engine, name):
        return False

    indices = sqlalchemy.inspect(engine).get_indexes(name)
    return indices and len(indices) > 0


def run_sql(engine, sql):
    c = engine.connect()
    sql = (sql)
    result = c.execute(sql)
    return result


def list_to_sql_format(_list):
    """
    把list转成sql中in要求的格式
    ['a','b','c'] => " 'a','b','c' "
    """
    if type(_list) != list: _list = [_list]
    data = ["\'" + one + "\'" for one in _list]
    return ','.join(data)


def create_db_index(engine, table_name, df):
    if is_table_index_exist(engine, table_name): return

    # 创建索引，需要单的sql处理
    index_sql = None
    if "ts_code" in df.columns and "trade_date" in df.columns:
        index_sql = "create index {}_code_date on {} (ts_code,trade_date);".format(table_name, table_name)
    if "ts_code" in df.columns and "ann_date" in df.columns:
        index_sql = "create index {}_code_date on {} (ts_code,ann_date);".format(table_name, table_name)

    if not index_sql: return

    start_time = time.time()
    engine.execute(index_sql)
    logger.debug("在表[%s]上创建索引，耗时: %.2f %s", table_name, time.time() - start_time, index_sql)


def get_start_date(table_name,date_column_name, db_engine, where=None):
    """
    如果表存在，就返回关键日期字段中，最后的日期，
    这个函数主要用于帮助下载后续日期的数据。
    如果表不存在，返回20080101
    :return:
    """

    if not is_table_exist(db_engine, table_name):
        logger.debug("表[%s]在数据库中不存在，返回默认最早开始日期[%s]", table_name, EALIEST_DATE)
        return EALIEST_DATE

    table_name = table_name
    if where:
        df = pd.read_sql('select max({}) from {} where {}'.format(date_column_name, table_name, where), db_engine)
    else:
        df = pd.read_sql('select max({}) from {}'.format(date_column_name, table_name), db_engine)
    assert len(df) == 1
    latest_date = df.iloc[:, 0].item()
    if latest_date is None:
        logger.debug("表[%s]中无数据，返回默认最早开始日期[%s]", table_name, EALIEST_DATE)
        return EALIEST_DATE

    # 日期要往后错一天，比DB中的
    latest_date = utils.tomorrow(latest_date)
    logger.debug("数据库中表[%s]的最后日期[%s]为：%s", table_name, date_column_name, latest_date)
    return latest_date