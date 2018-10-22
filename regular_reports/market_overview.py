from data_access import get_data
import datetime
import pandas as pd
import back_test.model.constant as c
start_date = datetime.date(2017,1,1)
end_date = datetime.date.today()
# name_code = 'sr'
# df_future = pd.read_excel('../data/SR_future.xlsx')
# tmp = df_future.groupby(['交易日期'])
# df_future_holding = pd.DataFrame(df_future.groupby(['dt_date'])['holding_volume'].sum()).reset_index()
# df_future_holding['dt_date'] = df_future_holding['dt_date'].apply(lambda x:x.date())
# df =get_data.commodity_option_market_overview(start_date,end_date,name_code)
# df = pd.merge(df,df_future_holding,on=c.Util.DT_DATE)
# df['pct_trading'] = df['option_trading_volume']/df['future_trading_volume']
# df['pct_holding'] = df['option_holding_volume']/df['holding_volume']
# print(df)
# df.to_csv('../data/market_overview'+str(name_code)+'.csv')
#
name_code = 'm'
df =get_data.commodity_option_market_overview(start_date,end_date,name_code)
df['pct_trading'] = df['option_trading_volume']/df['future_trading_volume']
df['pct_holding'] = df['option_holding_volume']/df['future_holding_volume']
print(df)
df.to_csv('../data/market_overview'+str(name_code)+'.csv')

# name_code = 'cu'
# df =get_data.commodity_option_market_overview(start_date,end_date,name_code)
# df['pct_trading'] = df['option_trading_volume']/df['future_trading_volume']
# df['pct_holding'] = df['option_holding_volume']/df['future_holding_volume']
# print(df)
# df.to_csv('../data/market_overview'+str(name_code)+'.csv')
