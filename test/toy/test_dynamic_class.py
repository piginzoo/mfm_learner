# https://www.drunkdream.com/2018/07/26/python-dynamic-create-class/
# https://community.backtrader.com/topic/1676/dynamically-set-params-and-lines-attribute-of-class-pandasdata/8
from backtrader.feeds import PandasData
import pandas as pd


def create_data_feed_class(lines, params):
    return type('PandasDataFeed', (PandasData,), {'lines': lines, 'params': params})


# ----------------------------------------------------------------------

class A():
    p2 = "test2"


class B(A):
    p1 = "test1"

    print("__class__:@p1", p1.__hash__())

    def __init__(self):
        print("__init__:", self.p1, self.p2)
        print("__init__:@p1", self.p1.__hash__())


# 测试一下，类的和实例的变量，这样定义的效果
print("B.p1", B.p1)
print("B.p2", B.p2)
b = B()
b.p1 = "test instance"
print("B.p1", b.p1)
print("B.p1:@p1", b.p1.__hash__())
print("B().p1", B.p1)
print("B().p1", B.p1.__hash__())
print("B().p2", b.p2)
print(dir(b))
# 结论是，实例化后self指向class的，但是一旦重新给self负值，self的就独立了
print("--------------------------------------------------------")

df = [
    ['000001.SZ', '2016-06-24', 0.165260, 0.002198, 0.085632, -0.078074, 0.173832, 0.214377, 0.068445],
    ['000001.SZ', '2016-06-25', 0.165537, 0.003583, 0.063299, -0.048674, 0.180890, 0.202724, 0.081748],
    ['000001.SZ', '2016-06-26', 0.135215, 0.010403, 0.059038, -0.034879, 0.111691, 0.122554, 0.042489],
]
df = pd.DataFrame(df, columns=["code", "datetime", "open", "close", "high", "low", "pct_chg", "clv", "mv"])
df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d')  # 时间为日期格式，tushare是str
d1 = PandasData(dataname=df)
PandasDataClass = create_data_feed_class(lines=("clv","mv","@aaaaaaaaaaaaaaa"),
                                         params=(("clv", -1),("mv",-1),("@aaaaaaaaaaaaaaa",-1)))
d2 = PandasDataClass(dataname=df)

print(dir(d1.lines))
print(dir(d2.lines))

# python -m test.toy.test_dynamic_class
