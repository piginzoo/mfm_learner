from example import factor_analyzer
from test.unitest import test_utils

# pytest test/unitest/test_factor_analyzer.py -s
def test_factor_returns_regression():
    df_factor_data = test_utils.generate_factor_data()
    df_tavlues,df_factor_returns = factor_analyzer.factor_returns_regression(df_factor_data)
    print(df_tavlues.head(3))
    print(df_factor_returns.head(3))
