from example import factor_analyzer
from test.unitest import test_utils

# pytest test/unitest/test_factor_analyzer.py -s
def test_factor_returns_regression():
    df_factor_data = test_utils.generate_factor_data()
    results = factor_analyzer.factor_returns_regression(df_factor_data)
    print(results)
