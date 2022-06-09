"""
把因子创建，因子因子检验，因子回测，都全部运行一遍
"""
import argparse
import time

from mfm_learner.example import factor_creator, factor_analyzer, factor_backtester
from mfm_learner.utils import utils
import logging

logger = logging.getLogger(__name__)

def main(factor_name, start_date, end_date, index_code, stock_num,period):
    factor_creator.main(factor_name, start_date, end_date, index_code, stock_num)
    logger.debug("\n\n\t\t%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n\n")
    factor_analyzer.main([factor_name], start_date, end_date, index_code, [period], stock_num)
    logger.debug("\n\n\t\t%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n\n")
    factor_backtester.main(start_date, end_date, index_code, period, stock_num, factor_name, risk=False, atr_period=15, atr_times=3)

# python -m mfm_learner.example.factor_main
"""
python -m mfm_learner.example.factor_main \
    --factor momentum_10d \
    --start 20180101 \
    --end 20191230 \
    --num 50 \
    --period 20 \
    --index 000905.SH 
"""
if __name__ == '__main__':
    utils.init_logger()

    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--factor', type=str, help="单个因子名、多个（逗号分割）、所有（all）")
    parser.add_argument('-s', '--start', type=str, help="开始日期")
    parser.add_argument('-e', '--end', type=str, help="结束日期")
    parser.add_argument('-i', '--index', type=str, help="股票池code")
    parser.add_argument('-p', '--period', type=int, help="调仓周期，多个的话，用逗号分隔")
    parser.add_argument('-n', '--num', type=int, help="股票数量")
    args = parser.parse_args()
    main(args.factor,
         args.start,
         args.end,
         args.index,
         args.num,
         args.period)