import datetime
import QuantLib as ql
from back_test.deprecated.BktOptionSet import BktOptionSet

from OptionStrategyLib.OptionStrategy.bkt_strategy_ivbymoneyness import BktStrategyMoneynessVol
from Utilities import admin_write_util as admin
from back_test.deprecated.BktUtil import BktUtil
from data_access.get_data import get_50option_mktdata, get_comoption_mktdata

start_date = datetime.date(2018,8,27)
end_date = datetime.date(2018,8,31)

calendar = ql.China()
daycounter = ql.ActualActual()
util = BktUtil()


optionMetrics = admin.table_option_metrics()
conn = admin.conn_metrics()

name_option = 'sr'
print(name_option)
df_option_metrics = get_comoption_mktdata(start_date,end_date,name_option)
bkt_optionset = BktOptionSet(df_option_metrics)
while bkt_optionset.index <= len(bkt_optionset.dt_list):
    evalDate = bkt_optionset.eval_date
    option_metrics = bkt_optionset.collect_option_metrics()
    try:
        for r in option_metrics:
            res = optionMetrics.select((optionMetrics.c.id_instrument == r['id_instrument'])
                                       &(optionMetrics.c.dt_date == r['dt_date'])).execute()
            if res.rowcount > 0:
                optionMetrics.delete((optionMetrics.c.id_instrument == r['id_instrument'])
                                       &(optionMetrics.c.dt_date == r['dt_date'])).execute()
            conn.execute(optionMetrics.insert(), r)
        print(evalDate, ' : option metrics -- inserted into data base succefully')
    except Exception as e:
        print(e)
        pass
    if evalDate == bkt_optionset.end_date:
        break
    bkt_optionset.next()


name_option = 'm'
print(name_option)
df_option_metrics = get_comoption_mktdata(start_date,end_date,name_option)
bkt_optionset = BktOptionSet(df_option_metrics)

while bkt_optionset.index <= len(bkt_optionset.dt_list):
    evalDate = bkt_optionset.eval_date
    option_metrics = bkt_optionset.collect_option_metrics()
    try:
        for r in option_metrics:
            res = optionMetrics.select((optionMetrics.c.id_instrument == r['id_instrument'])
                                       &(optionMetrics.c.dt_date == r['dt_date'])).execute()
            if res.rowcount > 0:
                optionMetrics.delete((optionMetrics.c.id_instrument == r['id_instrument'])
                                       &(optionMetrics.c.dt_date == r['dt_date'])).execute()
            conn.execute(optionMetrics.insert(), r)
        print(evalDate, ' : option metrics -- inserted into data base succefully')
    except Exception as e:
        print(e)
        pass
    if evalDate == bkt_optionset.end_date:
        break
    bkt_optionset.next()

print('50etf')
df_option_metrics = get_50option_mktdata(start_date,end_date)
bkt_optionset = BktOptionSet(df_option_metrics)

while bkt_optionset.index <= len(bkt_optionset.dt_list):
    evalDate = bkt_optionset.eval_date
    option_metrics = bkt_optionset.collect_option_metrics()
    try:
        for r in option_metrics:
            res = optionMetrics.select((optionMetrics.c.id_instrument == r['id_instrument'])
                                       &(optionMetrics.c.dt_date == r['dt_date'])).execute()
            if res.rowcount > 0:
                optionMetrics.delete((optionMetrics.c.id_instrument == r['id_instrument'])
                                       &(optionMetrics.c.dt_date == r['dt_date'])).execute()
            conn.execute(optionMetrics.insert(), r)
        print(evalDate, ' : option metrics -- inserted into data base succefully')
    except Exception as e:
        print(e)
        pass
    if evalDate == bkt_optionset.end_date:
        break
    bkt_optionset.next()

""" Calculate ATM Implied Volatility"""
print('50 atm iv')
df_option_metrics = get_50option_mktdata(start_date, end_date)

bkt = BktStrategyMoneynessVol(df_option_metrics)
bkt.set_min_holding_days(8) # TODO: 选择期权最低到期日
res = bkt.ivs_mdt1_run()
table = admin.table_option_atm_iv()
for r in res:
    try:
        conn.execute(table.insert(), r)
        # r.to_sql(name='option_atm_iv', con=engine, if_exists='append', index=False)
    except Exception as e:
        print(e)
        continue