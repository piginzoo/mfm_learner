# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 08:16:05 2020

@author: 12767
"""

import pandas as pd
import tushare as ts

pro = ts.pro_api('')
import statsmodels.api as sm


# 回归函数，返回α
def cal_aaa(df_buff):
    df_buff['index-rf'] = df_buff['index'] - df_buff['rf']
    df_buff['stock-rf'] = df_buff['pct_chg'] - df_buff['rf']
    model = sm.OLS(df_buff['stock-rf'], sm.add_constant(df_buff[['index-rf', 'SMB', 'HML']].values))
    result = model.fit()
    # print(result.params)
    print(result.summary())
    return result.params[0]


def analyze():
    # %%获取一段时间内的历史交易日
    df_cal = pro.trade_cal(start_date='20190101', end_date='20200801')
    df_cal = df_cal.query('(exchange=="SSE") & (is_open==1)')  # 筛选，清除非交易日
    Date = df_cal.cal_date.tolist()

    # 挑选出所需跨度的交易日
    month_trade_days = []
    i0 = 0
    while i0 < len(Date) - 1:
        # if Date[i0][5]!=Date[i0+1][5]:
        month_trade_days.append(Date[i0])
        i0 += 1
    month_trade_days.append(Date[-1])

    # %%提取出无风险利率
    rf = pd.read_excel('RF.xlsx')
    month_rf = []
    i0 = 0
    while (i0 < len(rf['Clsdt'])):
        if rf['Clsdt'][i0].replace('-', '') in month_trade_days:
            month_rf.append(rf['Nrrdaydt'][i0] / 100)
        i0 += 1
    # %%
    data_buff = pd.DataFrame()
    data_buff['trade_date'] = month_trade_days
    data_buff['rf'] = month_rf

    # 获取指数收益率信息
    index = pro.index_daily(ts_code='000002.SH', start_date='20190101', end_date='20200731')
    index = index.drop(['close', 'open', 'high', 'ts_code', 'low', 'change', 'pre_close', 'vol', 'amount'], axis=1)
    index = index.rename(columns={
        'pct_chg': 'index'})
    index['index'] = index['index'] / 100
    data_buff = pd.merge(data_buff, index, on='trade_date', how='inner')

    # 提取另外两个因子序列
    two_factor = pd.read_excel('three_factor_model.xlsx')
    data_buff['SMB'] = two_factor['SMB']
    data_buff['HML'] = two_factor['HML']

    # %%遍历所有股票，计算每只股票的α
    # 获取所有股票的信息
    stock_information = pro.stock_basic(exchange='', list_status='L',
                                        fields='ts_code,symbol,name,area,industry,list_date')
    aerfa_list = []
    # %%输出挑选出来的股票的详细回归信息
    aerfa_list = []
    stock_list = ['300156.SZ',
                  '300090.SZ',
                  '600175.SH',
                  '002220.SZ',
                  '002370.SZ',
                  '300677.SZ',
                  '600685.SH',
                  '600095.SH',
                  '603069.SH',
                  '601066.SH']
    i0 = 0
    while (i0 < len(stock_list)):
        stock = stock_list[i0]
        df = pro.daily(ts_code=stock, start_date='20190101', end_date='20200731')
        df_buff = pd.merge(data_buff, df, on='trade_date', how='inner')
        df_buff = df_buff.drop(['close', 'open', 'high', 'low', 'pre_close', 'change', 'vol', 'amount'], axis=1)
        if len(df_buff['rf']) == 0:
            aerfa_list.append(99)
        else:
            aerfa_list.append(cal_aaa(df_buff))
        print(stock)
        i0 += 1
    # %%********此处循环3000只股票
    i0 = 0
    while (i0 < len(stock_information['ts_code'])):
        stock = stock_information['ts_code'][i0]
        df = pro.daily(ts_code=stock, start_date='20190101', end_date='20200731')
        df_buff = pd.merge(data_buff, df, on='trade_date', how='inner')
        df_buff = df_buff.drop(['close', 'open', 'high', 'low', 'pre_close', 'change', 'vol', 'amount'], axis=1)
        if len(df_buff['rf']) == 0:
            aerfa_list.append(99)
        else:
            aerfa_list.append(cal_aaa(df_buff))
        print(stock)
        i0 += 1

    # %%保存数据
    stock_information['aerfa'] = aerfa_list
    stock_information.to_excel('stock_information.xlsx')


if __name__ == '__main__':
    analyze()
