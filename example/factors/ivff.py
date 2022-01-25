# coding: utf-8

import logging

from datasource import datasource_utils
from example.factors.factor import Factor

logger = logging.getLogger(__name__)


"""
所谓"特质波动率"： 就是源于一个现象"低特质波动的股票，未来预期收益更高"。

参考：
- https://www.joinquant.com/view/community/detail/b27081ecc7bccfc7acc484f8a63e2459
- https://zhuanlan.zhihu.com/p/30158144
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

    def __init__(self):
        super().__init__()

    def calculate(self, stock_codes, start_date, end_date, df_daily=None):
        if df_daily is None:
            df_daily = datasource_utils.load_daily_data(self.datasource, stock_codes, start_date, end_date)

        # 计算CLV因子
        df_daily['CLV'] = ((df_daily['close'] - df_daily['low']) - (df_daily['high'] - df_daily['close'])) / (
                df_daily['high'] - df_daily['low'])
        # 处理出现一字涨跌停
        df_daily.loc[(df_daily['high'] == df_daily['low']) & (df_daily['open'] > df_daily['pre_close']), 'CLV'] = 1
        df_daily.loc[(df_daily['high'] == df_daily['low']) & (df_daily['open'] < df_daily['pre_close']), 'CLV'] = -1

            '''
            计算年化的波动率
            计算方法如下：
            new_std = sqrt((init_std**2 + (1+init_ret)**2)**ret_freq - (1+init_ret)**(2*ret_freq))
            init_std和init_ret使用样本标准差和样本均值计算
            @param:
                rets: 需要计算年化波动率的收益率序列，为pd.Series类型
                ret_freq: 一年的区间数（例如，12表示月度数据年化，250表示日数据年化）
            @return:
                按照上述方法计算的年化波动率
            '''
            init_std = rets.std()
            init_ret = rets.mean()
            return sqrt((init_std ** 2 + (1 + init_ret) ** 2) ** ret_freq - (1 + init_ret) ** (2 * ret_freq))

        df_daily = datasource_utils.reset_index(df_daily)
        factors = df_daily['CLV']
        logger.debug("一共加载%s~%s %d条 CLV 数据", start_date, end_date, len(factors))

        return factors
