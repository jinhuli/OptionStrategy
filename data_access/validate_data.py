from Utilities import admin_util as admin
import pandas as pd
from sqlalchemy import inspect
import datetime


def get_valid_cf_datetime_set(date: datetime.date):
    ret = set()
    year = date.year
    month = date.month
    day = date.day
    ret.add(datetime.datetime(year, month, day, 9, 25))
    # 9:30 - 9:59
    for i in range(30, 60):
        ret.add(datetime.datetime(year, month, day, 9, i))
    # 10:00 - 10:59
    for i in range(60):
        ret.add(datetime.datetime(year, month, day, 10, i))
    # 11:00 -11:29
    for i in range(30):
        ret.add(datetime.datetime(year, month, day, 11, i))
    # 13:00 -13:59
    for i in range(60):
        ret.add(datetime.datetime(year, month, day, 13, i))
    # 14:00 -14:59
    for i in range(60):
        ret.add(datetime.datetime(year, month, day, 14, i))
    ret.add(datetime.datetime(year, month, day, 15, 0))
    return ret


def validate_date(date: datetime.date, daily_df):
    option_mktdata_intraday = admin.table_option_mktdata_intraday()
    session = admin.session_intraday()
    query = session.query(option_mktdata_intraday).filter(option_mktdata_intraday.c.dt_date == date)
    df = pd.read_sql(query.statement, query.session.bind)
    if df.isnull().values.any():
        print("NAN - intraday - date: {0}".format(date))
    for index, row in daily_df.iterrows():
        id_instrument = row['id_instrument']
        product_df = df[df.id_instrument == id_instrument].reset_index(drop=True)
        if product_df.shape[0] < 10:
            print("FATAL -  id: {0}, date: {1} only {2} rows, miss data.".format(id_instrument, date, product_df.shape[0]))
            continue
        datetime_set = get_valid_cf_datetime_set(date)
        for _, p in product_df.iterrows():
            if p.dt_datetime not in datetime_set:
                print("INVALID - id: {0}, datetime {1}"
                      .format(id_instrument, p.dt_datetime))
            else:
                datetime_set.remove(p.dt_datetime)
        for d in sorted(datetime_set):
            print("MISS - id: {0}, datetime: {1}".format(id_instrument, d))


options_mktdata = admin.table_options_mktdata()
mktdata_session = admin.session_mktdata()
date_list_result = mktdata_session.query(options_mktdata.c.dt_date).distinct().all()
date_list = [r[0] for r in date_list_result]
for date in date_list:
    print("INFO Checking date {}".format(date))
    q = mktdata_session.query(options_mktdata).filter(options_mktdata.c.dt_date == date)
    daily_df = pd.read_sql(q.statement, q.session.bind)
    if daily_df.isnull().values.any():
        print("NAN - daily - date: {0}".format(date))
    validate_date(date, daily_df)