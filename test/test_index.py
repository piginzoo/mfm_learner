import numpy as np
import pandas as pd

#data = np.random.randint(0, 100, (10, 1))
#index = [(i, "a" + str(i)) for i in range(10)]

# 参考 https://www.cnblogs.com/traditional/p/11967360.html
data = np.random.randint(0, 100, (10, 3))
data = pd.DataFrame(data, columns=["A", "B", "C"])
data = data.set_index(["A","B"])
print(data)
exit()
data = data.unstack()
data = data.rename_axis(columns=None)
print(data)
# python test_index.py
