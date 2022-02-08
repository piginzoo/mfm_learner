import logging

import sqlalchemy
import time
import pandas as pd
import os
import argparse

from utils import utils
from utils.tushare_download.download_utils import is_table_index_exist, is_table_exist

ROWS = None

logger = logging.getLogger(__name__)



def filter_duplicated(df, table_name, db_engine):
    if 'trade_date' in df.columns:
        date_column = 'trade_date'
    elif 'ann_date' in df.columns:
        date_column = 'ann_date'
    else:
        return df

    df = df.sort_values(date_column)
    latest_date_in_file = df.head(1)[date_column].item()  # 文件中最老日期
    sql = 'select * from {} where {}>=\'{}\''.format(table_name, date_column, latest_date_in_file)
    logger.debug("SQL: %s", sql)

    if not is_table_exist(db_engine, table_name):
        logger.debug("表[%s]在数据库中不存在",table_name)
        return df

    df_db = pd.read_sql(sql, db_engine)
    if len(df_db) == 0:
        logger.debug("导入文件和数据库，没有交集数据")
        return df

    logger.debug("导入文件和数据库，有交集数据%d条", len(df_db))

    old_size = len(df)
    df = df[~df[date_column].isin(df_db[date_column])]
    logger.debug("从文件dataframe中剔除掉%d条重复数据", old_size - len(df))
    return df


def import_dir(root_dir):
    # 加载文件夹中的所有csv文件
    files = []
    for dir in os.listdir(root_dir):
        csv_dir = os.path.join(root_dir, dir)
        if not os.path.isdir(csv_dir):
            continue
        csv_files = os.listdir(csv_dir)
        for file in csv_files:
            _, subfix = os.path.splitext(file)
            if subfix != ".csv": continue
            files.append(os.path.join(csv_dir, file))

    for i, file in enumerate(files):
        # 入库，指定字段的类型（没有就忽略），指定成VARHCAR原因是要建索引，不能为Text类型
        table_name = os.path.splitext(os.path.split(file)[-1])[0]
        table_name = table_name.replace(".", "_")
        import_file(file, table_name)


def import_file(file, table_name):
    start_time = time.time()
    db_engine = utils.connect_db()

    # 定义字段的数据类型为str，省的转成int
    dtype_dic = {'ts_code': str, 'trade_date': str, 'ann_date': str}
    df = pd.read_csv(file, dtype=dtype_dic, nrows=ROWS)
    if 'Unnamed: 0' in df.columns:
        df.drop(['Unnamed: 0'], axis=1, inplace=True)

    logger.debug("读取 [%.2f] 秒, [%d] 条, %s", time.time() - start_time, len(df), file)
    start_time = time.time()

    """
    # 防止重复导入
    看文件中最新的日期，和数据库中的最后日期最比较
    df_文件中的最新日期，理论上不应该在数据中存在，
    如果存在，那么就要舍弃这部分的导入。
    """

    df = filter_duplicated(df, table_name, db_engine)

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

    if is_table_index_exist(db_engine,table_name):
        logger.debug("表[%s]已经具有了索引，无需再创建", table_name)
        return

    # 创建索引，需要单的sql处理
    index_sql = None
    if "ts_code" in df.columns and "trade_date" in df.columns:
        index_sql = "create index {}_code_date on {} (ts_code,trade_date);".format(table_name, table_name)
    if "ts_code" in df.columns and "ann_date" in df.columns:
        index_sql = "create index {}_code_date on {} (ts_code,ann_date);".format(table_name, table_name)
    if index_sql:
        db_engine.execute(index_sql)
        logger.debug("索引 [%.2f] 秒: %s", time.time() - start_time, index_sql)


def main(dir, file, table_name):
    if dir:
        import_dir(dir)
    else:
        import_file(file, table_name)


"""
# 导入单个文件
python -m utils.tushare_download.db_importer \
    -f data/tushare_download/daily_hfq_20210630_20220204.csv \
    -t  daily_hfq

# 导入所有的
python -m utils.tushare_download.db_importer \
    -d data/tushare_data/data
"""
if __name__ == '__main__':
    utils.init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dir', type=str)
    parser.add_argument('-f', '--file', type=str)
    parser.add_argument('-t', '--table_name', type=str)
    args = parser.parse_args()

    main(args.dir, args.file, args.table_name)
