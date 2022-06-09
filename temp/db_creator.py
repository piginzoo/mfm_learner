"""
将"致敬大神"的csv数据导入到数据库:https://www.bilibili.com/video/BV1564y1b7PR?p=5
主要是做一下特殊处理：
- 把ts_code,start_date,end_date 变成字符串，
- 并建立联合索引: ts_code,trade_date,ann_date，三个里面有哪个就建立哪个
她的数据大概到2021.6
"""
import os
import time

import pandas as pd
import sqlalchemy

from mfm_learner.utils import utils

BASE_DIR = "data/tushare_data/data"
ROWS = None # 测试的话，改成100，全量改成None

# 加载文件夹中的所有csv文件
files = []
for dir in os.listdir(BASE_DIR):
    csv_dir = os.path.join(BASE_DIR, dir)
    if not os.path.isdir(csv_dir):
        continue
    csv_files = os.listdir(csv_dir)
    for file in csv_files:
        _, subfix = os.path.splitext(file)
        if subfix != ".csv": continue
        files.append(os.path.join(csv_dir, file))

# 读取每一个csv文件=>dataframe=>入库
db_engine = utils.connect_db()
for i,file in enumerate(files):

    start_time = time.time()

    # 定义字段的数据类型为str，省的转成int
    dtype_dic = {'ts_code': str, 'trade_date': str, 'ann_date': str}
    df = pd.read_csv(file, dtype=dtype_dic, nrows=ROWS)
    print("读取 [%.2f] 秒, [%d] 条, %s" % (time.time() - start_time, len(df), file));start_time = time.time()
    if len(df) == 0:
        print("[警告] 数据条数为0，不予导入")
        continue

    # 入库，指定字段的类型（没有就忽略），指定成VARHCAR原因是要建索引，不能为Text类型
    table_name = os.path.splitext(os.path.split(file)[-1])[0]
    table_name = table_name.replace(".", "_")
    dtype_dic = {
        'ts_code': sqlalchemy.types.VARCHAR(length=9),
        'trade_date': sqlalchemy.types.VARCHAR(length=8),
        'ann_date': sqlalchemy.types.VARCHAR(length=8)
    }
    df.to_sql(table_name, db_engine, index=False, if_exists='replace', dtype=dtype_dic, chunksize=1000)
    print("导入 [%.2f] 秒, df[%d条]=>db[表%s] " % (time.time()-start_time,len(df), table_name));start_time = time.time()

    # 创建索引，需要单的sql处理
    index_sql = None
    if "ts_code" in df.columns and "trade_date" in df.columns:
        index_sql = "create index {}_code_date on {} (ts_code,trade_date);".format(table_name, table_name)
    if "ts_code" in df.columns and "ann_date" in df.columns:
        index_sql = "create index {}_code_date on {} (ts_code,ann_date);".format(table_name, table_name)
    if index_sql:
        db_engine.execute(index_sql)
        print("索引 [%.2f] 秒: %s" % (time.time()-start_time, index_sql))

    print("------------------------------ 完成 %d/%d ------------------------------" %(i,len(files)))


# python -m mfm_learner.utils.db_creator
