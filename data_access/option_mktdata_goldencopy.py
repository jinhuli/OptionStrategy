import pandas as pd
import datetime
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker


def average(df):
    if df.empty: return -999.0
    sum = (df['amt_close'] * df['amt_trading_volume']).sum()
    vol = df['amt_trading_volume'].sum()
    if vol != 0:
        return sum / vol
    return -999.0

beg_date = datetime.date(2018, 4, 17)
end_date = datetime.date(2018, 4, 20)
# dc = DataCollection()
engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
engine_metrics = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/metrics', echo=False)
Session = sessionmaker(bind=engine)
sess = Session()
metadata = MetaData(engine)
options_mktdata = Table('options_mktdata', metadata, autoload=True)

engine_intraday = create_engine('mysql+pymysql://root:liz1128@101.132.148.152/mktdata_intraday', echo=False)
conn_intraday = engine_intraday.connect()
Session_intraday = sessionmaker(bind=engine_intraday)
sess_intraday = Session_intraday()
metadata_intraday = MetaData(engine_intraday)
option_mktdata_intraday = Table('option_mktdata_intraday', metadata_intraday, autoload=True)

query_mkt = sess.query(options_mktdata) \
    .filter(options_mktdata.c.datasource == 'wind')\
    .filter(options_mktdata.c.id_underlying == 'index_50etf')\
    .filter(options_mktdata.c.dt_date >= beg_date)\
    .filter(options_mktdata.c.dt_date <= end_date)

dataset = pd.read_sql_query(query_mkt.statement,query_mkt.session.bind)
dates = dataset['dt_date'].unique()

for date in dates:
    df = dataset[dataset['dt_date'] == date]

    for index, row in df.iterrows():
        cur = row['dt_date']
        next_day = row['dt_date'] + datetime.timedelta(days=1)
        id = row['id_instrument']
        windcode = row['code_instrument']
        query_intraday = sess_intraday.query(option_mktdata_intraday) \
            .filter(option_mktdata_intraday.c.datasource == 'wind') \
            .filter(option_mktdata_intraday.c.id_instrument == id) \
            .filter(option_mktdata_intraday.c.dt_datetime >= date) \
            .filter(option_mktdata_intraday.c.dt_datetime <= next_day)
        daily_df = pd.read_sql_query(query_intraday.statement,query_intraday.session.bind)
        if len(daily_df) < 240:
            print('intraday data length shorted : ',date,id,len(daily_df))
            # dt_date = date.strftime("%Y-%m-%d")
            # if len(df)>0:
            #     option_mktdata_intraday.delete(
            #         (option_mktdata_intraday.c.dt_datetime >= dt_date + " 09:30:00")&
            #         (option_mktdata_intraday.c.dt_datetime <= dt_date + " 15:00:00")&
            #         (option_mktdata_intraday.c.id_instrument == id)).execute()
            # db_data = dc.table_option_intraday().wind_data_50etf_option_intraday2(dt_date, windcode,id)
            # try:
            #     conn_intraday.execute(option_mktdata_intraday.insert(), db_data)
            #     print('option_mktdata_intraday -- inserted into data base succefully')
            # except Exception as e:
            #     print(e)

        df_morning_open_15min = daily_df.loc[lambda df: (df.dt_datetime >= datetime.datetime(cur.year, cur.month, cur.day, 9, 30, 0))&
                           (df.dt_datetime <= datetime.datetime(cur.year,cur.month,cur.day,9,45,0)),:]
        df_morning_close_15min = daily_df.loc[lambda df: (df.dt_datetime >= datetime.datetime(cur.year, cur.month, cur.day, 11, 15, 0))&
                           (df.dt_datetime <= datetime.datetime(cur.year,cur.month,cur.day,11,30,0)),:]
        df_afternoon_open_15min = daily_df.loc[lambda df: (df.dt_datetime >= datetime.datetime(cur.year, cur.month, cur.day, 13, 0, 0))&
                           (df.dt_datetime <= datetime.datetime(cur.year,cur.month,cur.day,13,15,0)),:]
        df_afternoon_close_15min = daily_df.loc[lambda df: (df.dt_datetime >= datetime.datetime(cur.year, cur.month, cur.day, 14, 45, 0))&
                           (df.dt_datetime <= datetime.datetime(cur.year,cur.month,cur.day,15,00,0)),:]
        df_morning = daily_df.loc[lambda df: (df.dt_datetime >= datetime.datetime(cur.year, cur.month, cur.day, 9, 30, 0))&
                           (df.dt_datetime <= datetime.datetime(cur.year,cur.month,cur.day,11,30,0)),:]
        df_afternoon = daily_df.loc[lambda df: (df.dt_datetime >= datetime.datetime(cur.year, cur.month, cur.day, 13, 0, 0))&
                           (df.dt_datetime <= datetime.datetime(cur.year,cur.month,cur.day,15,00,0)),:]

        amt_morning_open_15min = average(df_morning_open_15min)
        amt_morning_close_15min = average(df_morning_close_15min)
        amt_afternoon_open_15min = average(df_afternoon_open_15min)
        amt_afternoon_close_15min = average(df_afternoon_close_15min)
        amt_daily_avg = average(daily_df)
        amt_morning_avg = average(df_morning)
        amt_afternoon_avg = average(df_afternoon)
        df.loc[index,'amt_morning_open_15min'] = amt_morning_open_15min
        df.loc[index,'amt_morning_close_15min'] = amt_morning_close_15min
        df.loc[index,'amt_afternoon_open_15min'] = amt_afternoon_open_15min
        df.loc[index,'amt_afternoon_close_15min'] = amt_afternoon_close_15min
        df.loc[index,'amt_daily_avg'] = amt_daily_avg
        df.loc[index,'amt_morning_avg'] = amt_morning_avg
        df.loc[index,'amt_afternoon_avg'] = amt_afternoon_avg
        # print(df.loc[index])
        if row['amt_open'] == -999 or row['amt_close'] == -999:
            if row['amt_settlement'] == -999.0:
                print(row)
                print('No settlement and close data, No can do !!!!!')
            else:
                df.loc[index,'amt_open'] = row['amt_settlement']
                df.loc[index,'amt_close'] = row['amt_settlement']
                df.loc[index,'cd_remark'] = 'no trade volume'
    try:
        df.to_sql(name='options_mktdata_goldencopy', con=engine_metrics, if_exists = 'append', index=False)
        print(date,'inserted into database')
    except Exception as e:
        print(e)
        print(date)
        pass

