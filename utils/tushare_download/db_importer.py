import logging

import sqlalchemy
import time
import pandas as pd
import os
import argparse

from utils import utils

ROWS = None

logger = logging.getLogger(__name__)


def import_db(file, table_name):
    start_time = time.time()
    db_engine = utils.connect_db()

    # 定义字段的数据类型为str，省的转成int
    dtype_dic = {'ts_code': str, 'trade_date': str, 'ann_date': str}
    df = pd.read_csv(file, dtype=dtype_dic, nrows=ROWS)
    logger.debug("读取 [%.2f] 秒, [%d] 条, %s", ime.time() - start_time, len(df), file)
    start_time = time.time()
    if len(df) == 0:
        logger.warning("[警告] 数据条数为0，不予导入")
        return

    # 入库，指定字段的类型（没有就忽略），指定成VARHCAR原因是要建索引，不能为Text类型
    dtype_dic = {
        'ts_code': sqlalchemy.types.VARCHAR(length=9),
        'trade_date': sqlalchemy.types.VARCHAR(length=8),
        'ann_date': sqlalchemy.types.VARCHAR(length=8)
    }
    df.to_sql(table_name, db_engine, index=False, if_exists='append', dtype=dtype_dic, chunksize=1000)
    logger.debug("导入 [%.2f] 秒, df[%d条]=>db[表%s] ", time.time() - start_time, len(df), table_name)
    start_time = time.time()

    # 创建索引，需要单的sql处理
    index_sql = None
    if "ts_code" in df.columns and "trade_date" in df.columns:
        index_sql = "create index {}_code_date on {} (ts_code,trade_date);".format(table_name, table_name)
    if "ts_code" in df.columns and "ann_date" in df.columns:
        index_sql = "create index {}_code_date on {} (ts_code,ann_date);".format(table_name, table_name)
    if index_sql:
        db_engine.execute(index_sql)
        logger.debug("索引 [%.2f] 秒: %s", time.time() - start_time, index_sql)


if __name__ == '__main__':
    utils.init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=str)
    parser.add_argument('table_name', type=str)
    args = parser.parse_args()

    import_db(args.file, args.table_name)
