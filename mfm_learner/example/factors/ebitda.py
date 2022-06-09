"""
https://baike.baidu.com/item/EBITDA/7810909

税息折旧及摊销前利润，简称EBITDA，是Earnings Before Interest, Taxes, Depreciation and Amortization的缩写，
即未计利息、税项、折旧及摊销前的利润。

EBITDA受欢迎的最大原因之一是，EBITDA比营业利润显示更多的利润，公司可以通过吹捧EBITDA数据，把投资者在高额债务和巨大费用上面的注意力引开。

EBITDA = EBIT【息税前利润】 - Taxation【税款】+ Depreciation & Amortization【折旧和摊销】
- EBIT = 净利润+利息+所得税-公允价值变动收益-投资收益
- 折旧和摊销 = 资产减值损失

EBITDA常被拿来和现金流比较，因为它和净收入之间的差距就是两项对现金流没有影响的开支项目， 即折旧和摊销。
> 意思是说，这玩意，只考虑跟当期钱相关的，包含了利息，而且，刨除了那些假装要扣的钱（摊销和折旧）

我和MJ讨论后，我的理解是：
    咱不把借债的因素考虑进来，也不考虑缴税呢，也不让折旧摊销来捣乱，我就看我的赚钱能力（你丫甭管我借钱多少，咱就看，干出来的粗利润）
    当然是刨除了生产、销售成本之后的，不考虑息税和折旧。
------------------------------------------------

我查了tushare，有两个地方可以提供ebitda，一个是
    pro.fina_indicator(ts_code='600000.SH',fields='ts_code,ann_date,end_date,ebit,ebitda')
另外一个是，
    pro.income(ts_code='600000.SH',fields='ts_code,ann_date,end_date,ebit,ebitda')
income接口，是提供有效数据，fina_indicator提供的都是None，靠，tushare没有认真去清洗啊。
------------------------------
不过，我又去聚宽的：https://www.joinquant.com/help/api/help#factor_values:%E5%9F%BA%E7%A1%80%E5%9B%A0%E5%AD%90
他的《基础因子》api中，有ebotda，而且很全，是按照每日给出的，很棒。
唉，可惜他太贵了，他的API使用license是6000+/年（tushare 200/年）
更可怕的是，tushare的income的结果，和聚宽的基础因子接口，出来的结果是!!!不一样!!!的，不信可以自己去试一试（聚宽只能在他的在线实验室测试）
我信谁？
除非我去查年报，自己按照上面的"息税前利润（EBIT）+折旧费用+摊销费用 =EBITDA" 公式自己算一遍，好吧，逼急了，我就这么干。
------------------------------
目前，基于我只能用tushare的数据，我选择用income接口，然后按照交易日做ffill，我留个 TODO，将来这个指标要做进一步的优化！
"""
from mfm_learner.datasource import datasource_utils
from mfm_learner.example import factor_utils
from mfm_learner.example.factors.factor import Factor
from mfm_learner.utils import utils


class EBITDAFactor(Factor):
    """
    """

    def __init__(self):
        super().__init__()

    def name(self):
        return "ebitda_ttm"

    def calculate(self, stock_codes, start_date, end_date):
        start_date_1years_ago = utils.last_year(start_date, num=1)
        trade_dates = self.datasource.trade_cal(start_date, end_date)
        df_finance = self.datasource.income(stock_codes, start_date_1years_ago, end_date)

        # TODO 懒得重新下载fina_indicator，临时trick一下
        df_finance['end_date'] = df_finance['end_date'].apply(str)

        assert len(df_finance) > 0
        df = factor_utils.handle_finance_ttm(stock_codes,
                                             df_finance,
                                             trade_dates,
                                             col_name_value='ebitda',
                                             col_name_finance_date='end_date')

        df = datasource_utils.reset_index(df)
        return df['ebita_ttm']
