"""
账面市值比（book-to-market）：净资产/市值

[知乎参考](https://www.zhihu.com/question/23906290/answer/123700275)，计算方法：
- 1.账面市值比(BM)=股东权益/公司市值.
- 2.股东权益(净资产)=资产总额-负债总额 (每股净资产x流通股数)
- 3.公司市值=流通股数x每股股价
- 4.账面市值比(BM)=股东权益/公司市值=(每股净资产x流通股数)/(流通股数x每股股价)=每股净资产/每股股价=B/P=市净率的倒数
"""
from mfm_learner.datasource import datasource_utils
from mfm_learner.example.factors.factor import Factor


class BMFactor(Factor):
    """
    账面市值比（book-to-market）：净资产/市值 ,
    市净率（pb - price/book value ratio）的倒数：市值/净资产
    所以，我用市净率取一个倒数即可
    """

    def __init__(self):
        super().__init__()

    def name(self):
        return "bm"

    def cname(self):
        return "账面市值比"

    def calculate(self, stock_codes, start_date, end_date):
        df_basic = self.datasource.daily_basic(stock_codes, start_date, end_date)
        df_basic = datasource_utils.reset_index(df_basic)
        return 1 / df_basic['ps']  # ps是市净率，账面市值比是1/ps
