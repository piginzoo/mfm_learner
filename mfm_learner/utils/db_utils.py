import logging
import time

import sqlalchemy

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
