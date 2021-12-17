import numpy as np
import pandas as pd

#data = np.random.randint(0, 100, (10, 1))
#index = [(i, "a" + str(i)) for i in range(10)]

# 参考 https://www.cnblogs.com/traditional/p/11967360.html
data1 = np.random.randint(0, 100, (10, 3))
data1 = pd.DataFrame(data1, columns=["A", "B", "C"])
data = data1.set_index(["A","B"])
print(data.index.levels[0])
# 重现异常：TypeError: Must pass list-like as `names`.
# data = data.unstack()
# data = data.rename_axis(columns=None)
print("---------")
# print(data1)
data2 = data1.unstack()
# data2 = data2.reorder_levels([''])
data2.index.names=["i1",'i2']
print(data2)
data2 = data2.reorder_levels(['i2','i1'])
print(data2)

# python test_index.py
