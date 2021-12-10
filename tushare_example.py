import tushare as ts
import jqdatasdk
import utils

# token = utils.get_token()
# print("token:", token)
# ts.set_token(token)
# pro = ts.pro_api()
# df = ts.pro_bar(ts_code='000001.SZ', adj='hfq', start_date='20180101', end_date='20181011')
# print(df)
#

from jqdatasdk import auth
conf = utils.load_config()
conf = conf['jqdata']
print(conf)
auth(conf['uid'], conf['pwd'])
df = jqdatasdk.get_price('000300.XSHG', start_date='2015-01-01', end_date='2015-12-31', frequency='daily', fields=None, skip_paused=False, fq='pre', panel=True)
print(df)

# python tushare_example.py
