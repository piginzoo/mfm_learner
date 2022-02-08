import pandas as pd
from sqlalchemy import Table, MetaData

from utils import utils

data1 = [
    ['000001.SZ', '2016-06-24', 0.165260, 0.002198, 0.085632, -0.078074, 0.173832, 0.214377, 0.068445],
    ['000001.SZ', '2016-06-27', 0.165537, 0.003583, 0.063299, -0.048674, 0.180890, 0.202724, 0.081748],
    ['000001.SZ', '2016-06-28', 0.135215, 0.010403, 0.059038, -0.034879, 0.111691, 0.122554, 0.042489],
    ['000002.SH', '2016-06-29', 0.039431, 0.012271, 0.037432, -0.027272, 0.010902, 0.077293, -0.050667]
]
data1 = pd.DataFrame(data1, columns=["code", "datetime", "BP", "CFP", "EP", "ILLIQUIDITY", "REVS20", "SRMI", "VOL20"])
df = data1

engine = utils.connect_db()

metadata = MetaData(engine)

table = Table('test123', metadata, autoload=True)
table.delete().where((table.c.code == "000001.SZ") |
                     (table.c.datetime == '2016-06-29')).execute()
df.to_sql('test123', engine, index=False, if_exists='append')

# python -m test.toy.test_to_sql

"""
"""