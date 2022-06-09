"""
依据现有的daily数据，生成周和月的数据
"""
import argparse
import logging

import sqlalchemy
from tqdm import tqdm
from mfm_learner.datasource import datasource_factory, datasource_utils
from mfm_learner.utils import utils, db_utils

logger = logging.getLogger(__name__)
datasource = datasource_factory.get()


def main(code=None):
    db_engine = utils.connect_db()

    if code is None:
        stock_codes = utils.get_stock_codes(db_engine)
    else:
        stock_codes = [code]

    pbar = tqdm(total=len(stock_codes))
    for stock_code in stock_codes:
        df = datasource.daily(stock_code=stock_code)
        df = datasource_utils.reset_index(df, date_only=True, date_format="%Y%m%d")
        df = df.sort_index(ascending=True)

        if len(df)==0:
            logger.error("股票[%s]数据不存在，无法进行采样，请尽快下载其数据！",stock_code)
            continue

        process(db_engine, stock_code, df, "weekly")
        process(db_engine, stock_code, df, "monthly")
        pbar.update(1)

def precheck():
    """
    从库中加载数据，寻找到其最旧日期，如果不到
    :return:
    """
    pass


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
    logger.debug("导入股票 [%s] %d条数据=>db[表%s] ", code, len(df), table_name)


"""
python -m mfm_learner.utils.tushare_download.resample -c 603233.SH
"""
if __name__ == '__main__':
    utils.init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--code', type=str, default=None)
    args = parser.parse_args()

    main(args.code)
