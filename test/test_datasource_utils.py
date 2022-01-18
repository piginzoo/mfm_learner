# pytest  test/test_datasource_utils.py -s

import pandas as pd

from datasource import datasource_utils as dsu


def test_compile_industry():
    """
            index_code industry_name level industry_code is_pub parent_code
    10   801110.SI          家用电器    L1        330000   None           0
    22   801750.SI           计算机    L1        710000   None           0
    30   801022.SI          其他采掘    L2        210300   None      210000
    """
    data = pd.Series(["家用电器", "其他采掘", "计算机设备"])
    data = dsu.compile_industry(data)
    assert data[0] == "330000"
    assert data[1] == "210000"
    assert data[2] == "710000"
