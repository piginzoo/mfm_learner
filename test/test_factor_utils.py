# pytest  test/test_factor_utils.py -s
import math

import pandas as pd
import numpy as np
from example import factor_utils

allowed_error = 0.00000001

def test_pct_chg():
    data = pd.Series([1,1.1,1.21,1.331])
    returns = factor_utils.pct_chg(data,1)
    assert abs(returns[0] - 0.1)<allowed_error
    assert abs(returns[1] - 0.1) < allowed_error
    assert abs(returns[2] - 0.1) < allowed_error
    assert math.isnan(returns[3])