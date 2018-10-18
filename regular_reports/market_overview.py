from data_access import get_data
import datetime
start_date = datetime.date(2017,1,1)
end_date = datetime.date.today()
name_code = 'm'
df =get_data.commodity_option_market_overview(start_date,end_date,name_code)
print(df)
df.to_csv('../data/market_overview'+str(name_code)+'.csv')