import talib


def update_atr(df, time_period=15):
    df['atr'] = talib.ATR(df.high.values, df.low.values, df.close.values, timeperiod=time_period)
    return df

def risk_control(df):
    """
    :param df:
    :return:
    """
    # 获得回撤
    drawdown = get_drawdown()
