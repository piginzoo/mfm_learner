import logging

import pandas as pd

from utils import utils

utils.init_logger()

from datasource import datasource_factory, datasource_utils

logger = logging.getLogger(__name__)
datasource = datasource_factory.get()


# %%定义计算函数
def cal_smb_hml(df):
    """"
    参考：
    - https://zhuanlan.zhihu.com/p/55071842
    - https://zhuanlan.zhihu.com/p/341902943
    - https://zhuanlan.zhihu.com/p/21449852
    R_i = a_i + b_i * R_M + s_i * E(SMB) + h_i E(HMI) + e_i
    - R_i：是股票收益率
    - SMB：市值因子，用的就是市值信息, circ_mv
        SMB = (SL+SM+SH)/3 - (BL+BM+BH)/3
    - HMI：账面市值比，B/M，1/pb (PB是市净率=总市值/净资产)
        HMI = (BH+SH)/2 - (BL+SL)/2
    """

    # 划分大小市值公司
    median = df['circ_mv'].median()
    df['SB'] = df['circ_mv'].map(lambda x: 'B' if x >= median else 'S')

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

    """
    # 计算各组收益率, pct_chg:涨跌幅 , circ_mv:流通市值（万元）
    # 以SL为例子：Small+Low
    #    小市值+低账面市值比，的一组，比如100只股票，把他们的当期收益"**按照市值加权**"后，汇总到一起
    #    每期，得到的SL是一个数，
    # 除100是因为pct_chg是按照百分比计算的，比如pct_chg=3.5，即3.5%，即0.035
    # 组内按市值赋权平均收益率 = sum(个股收益率 * 个股市值/组内总市值)
    """
    R_SL = ((df_SL['pct_chg']/100) * (df_SL['circ_mv']/ df_SL['circ_mv'].sum()) ).sum()  # 这种写法和下面的5种结果一样
    R_SM = (df_SM['pct_chg'] * df_SM['circ_mv'] / 100).sum() / df_SM['circ_mv'].sum()    # 我只是测试一下是否一致，
    R_SH = (df_SH['pct_chg'] * df_SH['circ_mv'] / 100).sum() / df_SH['circ_mv'].sum()    # 大约在千分之几，也对，我做的是每日的收益率
    R_BL = (df_BL['pct_chg'] * df_BL['circ_mv'] / 100).sum() / df_BL['circ_mv'].sum()
    R_BM = (df_BM['pct_chg'] * df_BM['circ_mv'] / 100).sum() / df_BM['circ_mv'].sum()
    R_BH = (df_BH['pct_chg'] * df_BH['circ_mv'] / 100).sum() / df_BH['circ_mv'].sum()

    # 计算SMB, HML并返回
    # 这个没啥好说的，即使按照Fama造的公式，得到了smb，smb是啥？是当期的一个数
    smb = (R_SL + R_SM + R_SH - R_BL - R_BM - R_BH) / 3
    hml = (R_SH + R_BH - R_SL - R_BL) / 2
    return smb, hml, R_SL, R_SM, R_SH, R_BL, R_BM, R_BH


def calculate_factors(index_code="000905.SH", stock_num=50, start_date='20190101', end_date='20200801'):
    # %%计算并存储数据
    data = []

    # 获得股票池
    df_stocks = datasource.index_weight(index_code=index_code, start_date=start_date)
    logger.debug("获得股票池%d个股票", len(df_stocks))
    stocks = df_stocks.iloc[:, 0].unique().tolist()
    stocks = stocks[:10]
    logger.debug("保留股票池%d个股票分析使用", len(stocks))

    # 获取月线行情
    df_dailies = datasource.daily(start_date=start_date, end_date=end_date, stock_code=stocks)  # stock_code不能超过50个股票

    # 获取该日期所有股票的基本面指标，里面有市值信息
    df_basics = datasource.daily_basic(start_date=start_date, end_date=end_date, stock_code=stocks)

    # %%开始获取数据
    for date,df_daily in df_dailies.groupby('datetime'):
        # logger.debug("获得[%s]日股票交易数据%d条",date,len(df_daily))

        # 过滤出当日的基础数据
        df_basic = df_basics[df_basics['datetime']==date]

        # 数据融合——只保留两个表中公共部分的信息
        df = pd.merge(df_daily, df_basic, on=['code','datetime'], how='inner')

        # logger.debug("股票日交易数据与市场数据合并后(按日期[%s])，%d条", date, len(df))

        # 返回的 smb，hml，都是一个数（当期的一个数）
        # 这个数是对股票池中的每一个股票都是一样的，它是因子的收益率，可不是因子（也就是因子暴露，简称因子）噢
        # 每个股票的因子暴露得单独算，用回归来跑，每期，每支股票都有自己的风险暴露的
        smb, hml, sl, sm, sh, bl, bm, bh = cal_smb_hml(df)
        data.append([date, smb, hml, sl, sm, sh, bl, bm, bh])
        # logger.debug("计算完[%r] SMB=%.5f, HML=%.5f", date, smb, hml)

    # %%保存因子数据
    df_tfm = pd.DataFrame(data, columns=['datetime', 'SMB', 'HML', 'SL', 'SM', 'SH', 'BL', 'BM', 'BH'])
    df_tfm.to_excel('data/fama_ff3.xlsx')
    df_tfm = datasource_utils.reset_index(df_tfm, date_only=True)
    return df_tfm


# python -m fama.factor
if __name__ == '__main__':
    # index_code="000905.SH"，使用中证500股票池
    calculate_factors(index_code="000905.SH",
                      stock_num=10,
                      start_date='20200101',
                      end_date='20200201')
