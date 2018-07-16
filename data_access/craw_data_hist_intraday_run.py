# encoding: utf-8

from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
import datetime
from WindPy import w
from data_access.db_data_collection import DataCollection
from data_access.get_data import get_50option_mktdata
from back_test.BktOptionSet import BktOptionSet

w.start()

engine = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/mktdata', echo=False)
conn = engine.connect()
metadata = MetaData(engine)
options_mktdata_daily = Table('options_mktdata', metadata, autoload=True)
futures_mktdata_daily = Table('futures_mktdata', metadata, autoload=True)
futures_institution_positions = Table('futures_institution_positions', metadata, autoload=True)

engine_intraday = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/mktdata_intraday', echo=False)
conn_intraday = engine_intraday.connect()
metadata_intraday = MetaData(engine_intraday)
equity_index_intraday = Table('equity_index_mktdata_intraday', metadata_intraday, autoload=True)
option_mktdata_intraday = Table('option_mktdata_intraday', metadata_intraday, autoload=True)
index_daily = Table('indexes_mktdata', metadata, autoload=True)
engine_metrics = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/metrics', echo=False)
conn_metrics = engine_metrics.connect()
metadata_metrics = MetaData(engine_metrics)
optionMetrics = Table('option_metrics', metadata_metrics, autoload=True)

dc = DataCollection()
#####################################################################################
beg_date = datetime.date(2018, 4, 9)
end_date = datetime.date(2018, 4, 17)

date_range = w.tdays(beg_date, end_date, "").Data[0]
for dt in date_range:
    dt_date = dt.strftime("%Y-%m-%d")
    ############################################# MKT INTRADAY #############################################
    ## index mktdata intraday
    res = equity_index_intraday.select((equity_index_intraday.c.dt_datetime == dt_date + " 09:30:00") &
                                       (equity_index_intraday.c.id_instrument == 'index_50etf')).execute()
    if res.rowcount == 0:
        windcode = "510050.SH"
        id_instrument = 'index_50etf'
        db_data = dc.table_index_intraday().wind_data_equity_index(windcode, dt_date, id_instrument)
        try:
            conn_intraday.execute(equity_index_intraday.insert(), db_data)
            print('equity_index_intraday-50etf -- inserted into data base succefully')
        except Exception as e:
            print(e)
    res = equity_index_intraday.select((equity_index_intraday.c.dt_datetime == dt_date + " 09:30:00") &
                                       (equity_index_intraday.c.id_instrument == 'index_50sh')).execute()
    if res.rowcount == 0:
        windcode = "000016.SH"
        id_instrument = 'index_50sh'
        db_data = dc.table_index_intraday().wind_data_equity_index(windcode, dt_date, id_instrument)
        try:
            conn_intraday.execute(equity_index_intraday.insert(), db_data)
            print('equity_index_intraday-50sh -- inserted into data base succefully')
        except Exception as e:
            print(e)
    res = equity_index_intraday.select((equity_index_intraday.c.dt_datetime == dt_date + " 09:30:00") &
                                       (equity_index_intraday.c.id_instrument == 'index_300sh')).execute()
    if res.rowcount == 0:
        windcode = "000300.SH"
        id_instrument = 'index_300sh'
        db_data = dc.table_index_intraday().wind_data_equity_index(windcode, dt_date, id_instrument)
        try:
            conn_intraday.execute(equity_index_intraday.insert(), db_data)
            print('equity_index_intraday-300sh -- inserted into data base succefully')
        except Exception as e:
            print(e)
    res = equity_index_intraday.select((equity_index_intraday.c.dt_datetime == dt_date + " 09:30:00") &
                                       (equity_index_intraday.c.id_instrument == 'index_500sh')).execute()
    if res.rowcount == 0:
        windcode = "000905.SH"
        id_instrument = 'index_500sh'
        db_data = dc.table_index_intraday().wind_data_equity_index(windcode, dt_date, id_instrument)
        try:
            conn_intraday.execute(equity_index_intraday.insert(), db_data)
            print('equity_index_intraday-500sh -- inserted into data base succefully')
        except Exception as e:
            print(e)
    else:
        print(
            'equity index intraday -- already exists')

    ##option_mktdata_intraday
    res = option_mktdata_intraday.select(option_mktdata_intraday.c.dt_datetime == dt_date + " 09:30:00").execute()
    if res.rowcount == 0:
        df = dc.table_options().get_option_contracts(dt_date)
        for (idx_oc, row) in df.iterrows():
            db_data = dc.table_option_intraday().wind_data_50etf_option_intraday(dt_date, row)
            try:
                conn_intraday.execute(option_mktdata_intraday.insert(), db_data)
                print('option_mktdata_intraday -- inserted into data base succefully')
            except Exception as e:
                print(e)
    else:
        print('option intraday -- already exists')

    ####################CALCULATE OPTION METRICS#########################################
    # 50 ETF OPTION
    date = dt
    df_option_metrics = get_50option_mktdata(date,date)

    bkt_optionset = BktOptionSet('daily', df_option_metrics, 20)

    option_metrics = bkt_optionset.collect_option_metrics()
    try:
        for r in option_metrics:
            res = optionMetrics.select((optionMetrics.c.id_instrument == r['id_instrument'])
                                       & (optionMetrics.c.dt_date == r['dt_date'])).execute()
            if res.rowcount > 0:
                optionMetrics.delete((optionMetrics.c.id_instrument == r['id_instrument'])
                                     & (optionMetrics.c.dt_date == r['dt_date'])).execute()
            conn_metrics.execute(optionMetrics.insert(), r)
        print('option metrics -- inserted into data base succefully')
    except Exception as e:
        print(e)
