import numpy as np
import pandas as pd

#data = np.random.randint(0, 100, (10, 1))
#index = [(i, "a" + str(i)) for i in range(10)]

# 参考 https://www.cnblogs.com/traditional/p/11967360.html
# 造一个4行3列的df
data0 = np.random.randint(0, 100, (4, 3))
data0 = pd.DataFrame(data0, columns=["A", "B", "C"])

# 测试多索引
print("------------------------------")
print("原始数据")
print(data0)


# 测试多索引
print("------------------------------")
print("测试设置多索引")
data = data0.set_index(["A","B"])
print(data)
print("index:",data.index.names)

# 重现异常：TypeError: Must pass list-like as `names`.
# data = data.unstack()
# data = data.rename_axis(columns=None)
# print("---------")

# 测试stack
print("------------------------------")
print("测试stack：将数据的列“旋转”为行，默认操作为最内层")
data2 = data0.stack()
print(data2)
print("index:",data2.index.names)


# 测试unstack
print("------------------------------")
print("测试unstack：将数据的行“旋转”为列，默认操作为最内层")
data3 = data.unstack()
print(data3)
print("index:",data3.index.names)

print("------------------------------")
print("测试换index顺序：")
data4 = data.copy()
data4.index.names=["i1",'i2']
data4 = data4.reorder_levels(['i2','i1'])
print(data4)
print("index:",data4.index.names)

# python test_index.py
