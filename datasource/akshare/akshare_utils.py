import akshare as ak

def fund_daily(code, start_date, end_date):

    fund_em_open_fund_info_df = ak.fund_em_open_fund_info(fund=code, indicator="单位净值走势")
    print(fund_em_open_fund_info_df)