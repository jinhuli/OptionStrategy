from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
import pandas as pd
import datetime


engine = create_engine('mysql+pymysql://readonly:passw0rd@101.132.148.152/mktdata', echo=False)
metadata = MetaData(engine)

engine_intraday = create_engine('mysql+pymysql://readonly:passw0rd@101.132.148.152/mktdata_intraday', echo=False)
metadata_intraday = MetaData(engine_intraday)

engine_dzqh = create_engine('mysql+pymysql://readonly:passw0rd@101.132.148.152/dzqh', echo=False)
metadata_dzqh = MetaData(engine_dzqh)

def conn_dzqh():
    return engine_dzqh.connect()

def conn_mktdata():
    return engine.connect()

def conn_intraday():
    return engine_intraday.connect()

def session_dzqh():
    Session = sessionmaker(bind=engine_dzqh)
    return Session()

def session_mktdata():
    Session = sessionmaker(bind=engine)
    return Session()

def session_intraday():
    Session = sessionmaker(bind=engine_intraday)
    return Session()

def table_options_mktdata():
    return Table('options_mktdata', metadata, autoload=True)

def table_futures_mktdata():
    return Table('futures_mktdata', metadata, autoload=True)

def table_futures_institution_positions():
    return Table('futures_institution_positions', metadata, autoload=True)

def table_indexes_mktdata():
    return Table('indexes_mktdata', metadata, autoload=True)

def table_option_contracts():
    return Table('option_contracts', metadata, autoload=True)

def table_future_contracts():
    return Table('future_contracts', metadata, autoload=True)

def table_stocks_mktdata():
    return Table('stocks_mktdata', metadata, autoload=True)

def table_events():
    return Table('events', metadata, autoload=True)

def table_index_mktdata_intraday():
    return Table('equity_index_mktdata_intraday', metadata_intraday, autoload=True)

def table_option_mktdata_intraday():
    return Table('option_mktdata_intraday', metadata_intraday, autoload=True)

def table_cf_minute_1():
    return Table('cf_minute_1',metadata_dzqh, autoload=True)

def table_cf_daily():
    return Table('cf_day',metadata_dzqh, autoload=True)


def cf_minute(start_date, end_date, name_code):
    name_code = name_code.lower()
    table_cf = table_cf_minute_1()
    query = session_dzqh().query(table_cf.c.dt_datetime, table_cf.c.id_instrument, table_cf.c.dt_date,
                                       table_cf.c.amt_open, table_cf.c.amt_close, table_cf.c.amt_trading_volume). \
        filter((table_cf.c.dt_date >= start_date)&(table_cf.c.dt_date <= end_date)&(table_cf.c.name_code == name_code))
    df = pd.read_sql(query.statement, query.session.bind)
    # df = df[df['id_instrument'].str.contains("_")]
    return df

def get_index_minute(start_date, end_date, id_index):
    Index = table_index_mktdata_intraday()
    query = session_intraday().query(Index.c.dt_datetime, Index.c.id_instrument, Index.c.amt_close,
                                           Index.c.amt_trading_volume, Index.c.amt_trading_value) \
        .filter(Index.c.dt_datetime >= start_date).filter(Index.c.dt_datetime <= end_date) \
        .filter(Index.c.id_instrument == id_index)
    df = pd.read_sql(query.statement, query.session.bind)
    return df


def get_index_mktdata(start_date, end_date, id_index):
    Index_mkt = table_indexes_mktdata()
    query_etf = session_mktdata().query(Index_mkt.c.dt_date, Index_mkt.c.amt_close, Index_mkt.c.amt_open,
                                              Index_mkt.c.id_instrument, Index_mkt.c.amt_high,Index_mkt.c.amt_low,
                                        Index_mkt.c.amt_trading_volume,Index_mkt.c.amt_trading_value) \
        .filter(Index_mkt.c.dt_date >= start_date).filter(Index_mkt.c.dt_date <= end_date) \
        .filter(Index_mkt.c.id_instrument == id_index)
    df_index = pd.read_sql(query_etf.statement, query_etf.session.bind)
    return df_index

def get_50option_minute(start_date, end_date):
    OptionIntra = table_option_mktdata_intraday()
    query = session_intraday().query(OptionIntra.c.dt_datetime,
                                           OptionIntra.c.dt_date,
                                           OptionIntra.c.id_instrument,
                                           OptionIntra.c.amt_close,
                                           OptionIntra.c.amt_trading_volume,
                                           OptionIntra.c.amt_trading_value) \
        .filter(OptionIntra.c.dt_date >= start_date).filter(OptionIntra.c.dt_date <= end_date)
    df = pd.read_sql(query.statement, query.session.bind)
    return df

dt1 = datetime.date(2018, 5, 5)
dt2 = datetime.date(2018, 5, 7)
# data : 50etf 期权分钟数据
data = get_50option_minute(dt1, dt2)
print(data)
