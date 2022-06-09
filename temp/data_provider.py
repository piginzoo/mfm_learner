import logging
import tushare as ts
from mfm_learner import utils

logger = logging.getLogger(__name__)


def get(name):
    if name == "tushare":
        return Tushare()
    raise ValueError("unrecognized name: " + name)


class DataProvider():
    def bar(pro, code, start, end, adj='hfq'):
        pass

    def index_stocks(self, code,start,end):
        pass

    def basic(self, code, start, end):
        pass


class Tushare(DataProvider):
    def __init__(self):
        conf = utils.load_config()
        ts.set_token(conf['tushare']['token'])
        self.pro = ts.pro_api()
        logger.info("设置Tushare token: %s",conf['tushare']['token'][:10] + "......" )

    def index_stocks(self, code, start, end):
        # https://tushare.pro/document/2?doc_id=96
        df = self.pro.index_weight(index_code=code, start_date=start,end_date = end)
        df = df['con_code'].unique()
        logger.debug("获得日期%s~%s的指数%s的成分股：%d 个",start,end,code, len(df))
        return df

    def basic(self, code, start, end):
        df = self.pro.daily_basic(ts_code=code, start_date=start, end_date=end)
        return df

    def bar(self, code, start, end, adj='hfq'):
        """
        # https://tushare.pro/document/2?doc_id=95
        ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount
        """
        # 缓存，不过查了，"基础积分每分钟内最多调取500次，每次5000条数据，相当于23年历史，用户获得超过5000积分正常调取无频次限制。"
        # 感觉没必要了
        # if not conf.BAR_DIR: os.makedirs(conf.BAR_DIR)
        # bar_data_path = os.path.join(conf.BAR_DIR,code+".csv")
        # if os.path.exist(bar_data_path):
        # df = pd.read_csv(bar_data_path,parse_dates=True,infer_datetime_format=True)

        return self.pro.daily(ts_code=code, adj=adj, start_date=start, end_date=end,
                              field='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount')


class JQdata(DataProvider):
    def __init__(self):
        conf = utils.load_config()
        auth(conf['pid'], conf['pwd'])

    def bar(self, code, start, end, adj='hfq'):
        """
        https://www.joinquant.com/help/api/help#Stock:get_price
        - open 时间段开始时价格
        - close 时间段结束时价格
        - low 最低价
        - high 最高价
        - volume 成交的股票数量
        - money 成交的金额
        - factor 前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以factor, 比如: close/factor
        - high_limit 涨停价
        - low_limit 跌停价
        - avg 这段时间的平均价, 等于money/volume
        - pre_close 前一个单位时间结束时的价格, 按天则是前一天的收盘价, 按分钟这是前一分钟的结束价格
        - paused 布尔值, 这只股票是否停牌, 停牌时open/close/low/high/pre_close依然有值,都等于停牌前的收盘价, volume=money=0
        """
        import jqdatasdk
        df = jqdatasdk.get_price(code, start_date=start, end_date=end, frequency='daily',
                                 fields="trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
                                 skip_paused=False, fq='post', panel=True)
