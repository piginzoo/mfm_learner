# coding: utf-8

import logging

import statsmodels.formula.api as sm
import numpy as np
import pandas as pd
from datasource import datasource_utils
from example.factors.factor import Factor
from fama import fama_model

logger = logging.getLogger(__name__)

"""
所谓"特质波动率"： 就是源于一个现象"低特质波动的股票，未来预期收益更高"。

参考：
- https://www.joinquant.com/view/community/detail/b27081ecc7bccfc7acc484f8a63e2459
- https://www.joinquant.com/view/community/detail/1813dae5165ee3c5c81e2408d7fe576f
- https://zhuanlan.zhihu.com/p/30158144
- https://zhuanlan.zhihu.com/p/379585598
- https://mp.weixin.qq.com/s/k_2ltrIQ7jkgAKhDc7Vo2A
- https://blog.csdn.net/FightingBob/article/details/106791144
- https://uqer.datayes.com/v3/community/share/58db552a6d08bb0051c52451


特质波动率(Idiosyncratic Volatility, IV)与预期收益率的负向关系既不符合经典资产定价理论，
也不符合基于不完全信息的定价理论，因此学术界称之为“特质波动率之谜”。

该因子虽在多头部分表现略逊于流通市值因子，但在多空方面表现明显强于流通市值因子，
说明特质波动率因子具有很好的选股区分能力，并且在空头部分有良好的风险警示作用。

基于CAPM的特质波动率 IVCAPM: 就是基于CAMP的残差的年化标准差来衡量。
基于Fama-French三因子模型的特质波动率 IVFF3： 就是在IVCAMP的基础上，再剔除市值因子和估值因子后的残差的年化标准差来衡量。

------

说说实现，
    特质波动率，可以有多种实现方法，可以是CAMP市场的残差，也可以是Fama-Frech的残差，这里，我用的是FF3的残差，
    啥叫FF3的残差，就是，用Fama定义的模型，先去算因子收益，[参考](../../fama/factor.py)中，
    使用股票池（比如中证500），去算出来的全市场的SMB，HML因子，
    然后，就可以对某一直股票，比如"招商银行"，对他进行回归：r_i = α_i + b1 * r_m_i + b2 * smb_i + b3 * hml_i + e_i
    我们要的就是那个e_i，也就是这期里，无法被模型解释的'**特质**'。上式计算，用的是每天的数据，为何强调这点呢，是为了说明e_i的i，指的是每天。
    那波动率呢？
    就是计算你回测周期的内的标准差 * sqrt(T)，比如你回测周期是20天，那就是把招商银行这20天的特异残差求一个标准差，然后再乘以根号下20。
    这个值，是这20天共同拥有的一个"特异波动率"，对，这20天的因子暴露值，都一样，都是这个数！
    我是这么理解的，也不知道对不对，这些文章云山雾罩地不说人话都。
"""


class IVFFFactor(Factor):

    def __init__(self,index_code="000905.SH"):
        """
        :param index_code: 使用的指数股票池代码
        :param time_window: 计算
        """
        super().__init__()
        self.index_code = index_code

    def get_market(self, start_date, end_date):
        # 获取指数收益率信息
        df_index = self.datasource.index_daily(index_code=self.index_code, start_date=start_date, end_date=end_date)
        df_index = df_index[['datetime','pct_chg']]
        df_index = datasource_utils.reset_index(df_index,date_only=True)
        logger.debug("获得[%s]指数日收益，作为市场收益率:%d行", self.index_code, len(df_index))
        return df_index

    # TODO: 算标准差的时候，是每天都算一次么？类滑动窗口（TODO，可以用之前写的计算滑动窗酷的工具类，貌似用shift实现的）
    def ___calculate_residuals(self, residuals,time_window):
        result = []
        for i in range(residuals):
            sub_residuals = residuals[i:i + time_window]
            if len(sub_residuals) < time_window: result.append(np.nan)
            result.append(sub_residuals.std())
        return result

    def calculate(self, stock_codes, start_date, end_date):

        """
        各只股票信息
        """
        df_daily = datasource_utils.load_daily_data(self.datasource, stock_codes, start_date, end_date)
        df_daily = datasource_utils.reset_index(df_daily,date_only=True)

        """
        df_fama[date | smb, hml, sl, sm, sh, bl, bm, bh]
        index是date
        """
        df_fama = fama_model.calculate_factors(index_code=self.index_code,
                                               stock_num=10,
                                               start_date=start_date,
                                               end_date=end_date)

        df_market = self.get_market(start_date, end_date)

        # 合并市场数据到fama数据中
        df_fama = df_fama.merge(df_market,on=['datetime'], how='inner')


        """
        # 参考：
        - https://blog.csdn.net/CoderPai/article/details/82982146 
        - https://zhuanlan.zhihu.com/p/261031713
        formula里直接指定Y和X_i，就是dataframe中的列名，酷
        
        """
        results = []
        for name,df_stock in df_daily.groupby('code'):
            """
            date        smb         hml         stock1
            2016-06-24	0.165260	0.002198	0.085632	-0.078074	0.173832	0.214377	0.068445
            2016-06-27	0.165537	0.003583	0.063299	-0.048674	0.180890	0.202724	0.081748
            2016-06-28	0.135215	0.010403	0.059038	-0.034879	0.111691	0.122554	0.042489
            2016-06-29	0.068774	0.019848	0.058476	-0.049971	0.042805	0.053339	0.079592
            2016-06-30	0.039431	0.012271	0.037432	-0.027272	0.010902	0.077293	-0.050667
            ---------------------------------------------------------------------------------------------

            """

            df_data = df_fama.merge(df_stock,on=['datetime'], how='inner')

            """
            r_i = α_i + b1 * r_m_i + b2 * smb_i + b3 * hml_i + e_i 
            某一天（截面上），做回归，r_i,r_m_i，smb_i，hml_i 已知，回归后，得到e_i(残差)
            """
            ols_result = sm.ols(formula='pct_chg ~ market + smb + hml', data=df_data).fit()
            residuals = ols_result.resid
            df_data['vi'] = residuals
            logger.debug("计算完股票[%s]的残差：%d 条",name,len(residuals))
            results.append(df_data[['code','vi']])
        return pd.concat(results,axis=1)


        # df_residuals = self.___calculate_residuals(residuals,time_window)
        # df_residuals = datasource_utils.reset_index(df_residuals)
        # return df_residuals
