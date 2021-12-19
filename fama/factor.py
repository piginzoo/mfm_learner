# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 19:53:56 2020
https://www.cxyzjd.com/article/nv_144/108891000
@author: 12767
"""
import utils
utils.init_logger()
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import logging

logger = logging.getLogger(__name__)
sns.set()
mpl.rcParams['font.sans-serif'] = 'WenQuanYi Micro Hei'
pro = utils.tushare_login()

# %%定义计算函数
def cal_smb_hml(df):
    # 划分大小市值公司
    df['SB'] = df['circ_mv'].map(lambda x: 'B' if x >= df['circ_mv'].median() else 'S')
    # 求账面市值比：PB的倒数
    df['BM'] = 1 / df['pb']
    # 划分高、中、低账面市值比公司
    border_down, border_up = df['BM'].quantile([0.3, 0.7])
    df['HML'] = df['BM'].map(lambda x: 'H' if x >= border_up else 'M')
    df['HML'] = df.apply(lambda row: 'L' if row['BM'] <= border_down else row['HML'], axis=1)

    # 组合划分为6组
    df_SL = df.query('(SB=="S") & (HML=="L")')
    df_SM = df.query('(SB=="S") & (HML=="M")')
    df_SH = df.query('(SB=="S") & (HML=="H")')
    df_BL = df.query('(SB=="B") & (HML=="L")')
    df_BM = df.query('(SB=="B") & (HML=="M")')
    df_BH = df.query('(SB=="B") & (HML=="H")')

    # 计算各组收益率, pct_chg:涨跌幅 , circ_mv:流通市值（万元）
    R_SL = (df_SL['pct_chg'] * df_SL['circ_mv'] / 100).sum() / df_SL['circ_mv'].sum()
    R_SM = (df_SM['pct_chg'] * df_SM['circ_mv'] / 100).sum() / df_SM['circ_mv'].sum()
    R_SH = (df_SH['pct_chg'] * df_SH['circ_mv'] / 100).sum() / df_SH['circ_mv'].sum()
    R_BL = (df_BL['pct_chg'] * df_BL['circ_mv'] / 100).sum() / df_BL['circ_mv'].sum()
    R_BM = (df_BM['pct_chg'] * df_BM['circ_mv'] / 100).sum() / df_BM['circ_mv'].sum()
    R_BH = (df_BH['pct_chg'] * df_BH['circ_mv'] / 100).sum() / df_BH['circ_mv'].sum()

    # 计算SMB, HML并返回
    smb = (R_SL + R_SM + R_SH - R_BL - R_BM - R_BH) / 3
    hml = (R_SH + R_BH - R_SL - R_BL) / 2
    return smb, hml, R_SL, R_SM, R_SH, R_BL, R_BM, R_BH


def calculate_factors(index_code="000905.SH", stock_num = 50, start_date='20190101', end_date='20200801'):
    # %%计算并存储数据
    data = []

    # 获取一段时间内的历史交易日
    df_cal = pro.trade_cal(start_date=start_date, end_date=end_date)
    df_cal = df_cal.query('(exchange=="SSE") & (is_open==1)')  # 筛选，清除非交易日，SSE上交所/SZSE深交所，0休市，1交易
    trade_dates = df_cal.cal_date.tolist()
    logger.debug("得到 %r~%r %d 个交易日",start_date,end_date,len(df_cal))

    # 获得股票池
    df = pro.index_weight(index_code=index_code, start_date=start_date, end_date=end_date)
    logger.debug("获得股票池%d个股票",len(df))
    df = df.sample(frac=1) # shuffle
    df = df[:stock_num]
    stocks = ",".join(df['con_code'].unique().tolist())
    logger.debug("保留股票池%d个股票分析使用", len(df))

    # # %%挑选出需要的时间跨度交易日
    # 原代码，不知做什么用的，暂作保留
    # month_trade_days = []
    # i0 = 0
    # while i0 < len(trade_dates) - 1:
    #     # if trade_dates[i0][5]!=trade_dates[i0+1][5]:
    #     month_trade_days.append(trade_dates[i0])
    #     i0 += 1
    # month_trade_days.append(trade_dates[-1])

    # %%开始获取数据
    for date in trade_dates:
        # 获取月线行情
        df_daily = pro.daily(trade_date=date, ts_code=stocks) # ts_code不能超过50个股票
        # 获取该日期所有股票的基本面指标
        df_basic = pro.daily_basic(trade_date=date, ts_code=stocks)
        # 数据融合——只保留两个表中公共部分的信息
        df = pd.merge(df_daily, df_basic, on='ts_code', how='inner')
        smb, hml, sl, sm, sh, bl, bm, bh = cal_smb_hml(df)
        data.append([date, smb, hml, sl, sm, sh, bl, bm, bh])
        logger.debug("[%r] SMB=%2.f, HML=%.2f", date, smb, hml)

    # %%保存因子数据
    df_tfm = pd.DataFrame(data, columns=['trade_date', 'SMB', 'HML', 'SL', 'SM', 'SH', 'BL', 'BM', 'BH'])
    # df_tfm['trade_date'] = pd.to_datetime(df_tfm.trade_date)
    # df_tfm = df_tfm.set_index('trade_date')
    df_tfm.to_excel('data/three_factor_model.xlsx')


# python -m fama.factor
if __name__ == '__main__':
    calculate_factors(index_code="000905.SH",
                      stock_num = 10,
                      start_date='20200101',
                      end_date='20200201')
