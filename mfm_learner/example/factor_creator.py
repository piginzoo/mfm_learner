"""
用来生成因子数据，省的每次都算
"""
import argparse
import logging
import time

from mfm_learner.datasource import datasource_factory
from mfm_learner.example import factor_utils
from mfm_learner.example.factors.factor import Factor
from mfm_learner.utils import utils, dynamic_loader

datasource = datasource_factory.get()

logger = logging.getLogger(__name__)


def main(factor_name, start_date, end_date, index_code, stock_num):
    start_time = time.time()

    class_dict = dynamic_loader.dynamic_instantiation("example.factors", Factor)
    if factor_name == "all":
        for _, clazz in class_dict.items():
            factor = clazz()
            factor_name = factor.name()
            factor = dynamic_loader.create_factor_by_name(factor_name, class_dict)
            calculate_and_save(factor_name, factor, start_date, end_date, index_code, stock_num)
    else:
        factor = dynamic_loader.create_factor_by_name(factor_name, class_dict)
        if factor:
            calculate_and_save(factor_name, factor, start_date, end_date, index_code, stock_num)

    logger.info("合计处理因子耗时 %.2f 秒", time.time() - start_time)


def calculate_and_save(factor_name, factor, start_date, end_date, index_code, stock_num):
    stock_codes = datasource.index_weight(index_code, start_date, end_date)[:stock_num]

    start_time = time.time()
    df_factor = factor.calculate(stock_codes, start_date, end_date)
    logger.info("计算因子[%s]耗时 %.2f 秒", factor_name, time.time() - start_time)

    if type(df_factor) == list or type(df_factor) == tuple:
        # 如果factor返回的是多个dataframe，那么这个时候，需要重新取一下所有的名字，传入的fator_name只是其中的一个，是个引子
        factor_names = factor.name()
        # 处理像turnover这样，一次创建多个因子的情况
        for n, f in zip(factor_names, df_factor):
            # factor默认索引是datetime和code，为了保存数据库中，需要unindex
            f = f.reset_index()
            factor_utils.factor2db(name=n, factor=f)
    else:
        df_factor = df_factor.reset_index()
        factor_utils.factor2db(name=factor_name, factor=df_factor)


"""
python -m mfm_learner.example.factor_creator \
    --factor clv \
    --start 20080101 \
    --end 20220209 \
    --num 100000 \
    --index 000905.SH 

python -m mfm_learner.example.factor_creator \
    --factor all \
    --start 20210101 \
    --end 20210801 \
    --num 10 \
    --index 000905.SH 

python -m mfm_learner.example.factor_creator \
    --factor bm \
    --start 20180101 \
    --end 20191230 \
    --num 50 \
    --index 000905.SH 

"""
if __name__ == '__main__':
    utils.init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--factor', type=str, help="因子名|all是所有")
    parser.add_argument('-s', '--start', type=str, help="开始日期")
    parser.add_argument('-e', '--end', type=str, help="结束日期")
    parser.add_argument('-i', '--index', type=str, help="股票池code")
    parser.add_argument('-n', '--num', type=int, help="股票数量")
    args = parser.parse_args()

    main(args.factor,
         args.start,
         args.end,
         args.index,
         args.num)
