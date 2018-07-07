import datetime

from back_test.bkt_strategy_collar import BktStrategyCollar
from data_access.get_data import get_50option_mktdata, get_index_mktdata

start_date = datetime.date(2017, 10, 1)
end_date = datetime.date(2018, 1, 21)


"""Collect Mkt Date"""

df_option_metrics = get_50option_mktdata(start_date, end_date)
df_index_metrics = get_index_mktdata(start_date,end_date,'index_50etf')
bkt_strategy = BktStrategyCollar(df_option_metrics, df_index_metrics)
print(bkt_strategy.bkt_optionset.df_daily_state)