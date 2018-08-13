from sqlalchemy import *
import pandas as pd
from data_access.db_tables import DataBaseTables as dbt
from Utilities import admin_util as admin


def get_mktdata_future_c1(start_date, end_date, name_code):
    table_f = admin.table_futures_mktdata()
    query = admin.session_mktdata().query(table_f.c.dt_date, table_f.c.id_instrument,
                                          table_f.c.amt_close, table_f.c.amt_trading_volume). \
        filter((table_f.c.dt_date >= start_date) & (table_f.c.dt_date <= end_date)). \
        filter(table_f.c.name_code == name_code)
    df = pd.read_sql(query.statement, query.session.bind)
    df = df[df['id_instrument'].str.contains("_")]
    df = df.sort_values(by=['dt_date', 'amt_trading_volume'], ascending=False)
    df_rs = df.drop_duplicates(subset=['dt_date']).sort_values(by='dt_date', ascending=True).reset_index(drop=True)
    return df_rs


def get_mktdata_future(table_future, id_instrument, dt_start, dt_end):
    query = admin.session_mktdata().query(table_future.c.dt_date, table_future.c.id_instrument,
                                          table_future.c.amt_close, table_future.c.amt_trading_volume,
                                          table_future.c.amt_settlement) \
        .filter(table_future.c.dt_date >= dt_start).filter(table_future.c.dt_date <= dt_end) \
        .filter(table_future.c.id_instrument == id_instrument) \
        .filter(table_future.c.flag_night != 1)
    df = pd.read_sql(query.statement, query.session.bind)
    return df


def get_volume_option(table_option, id_underlying, dt_start, dt_end):
    query = admin.session_mktdata().query(table_option.c.dt_date, table_option.c.id_underlying,
                                          table_option.c.amt_strike, table_option.c.cd_option_type,
                                          func.sum(table_option.c.amt_holding_volume).label('total_holding_volume'),
                                          func.sum(table_option.c.amt_trading_volume).label('total_trading_volume')
                                          ) \
        .filter(table_option.c.dt_date >= dt_start).filter(table_option.c.dt_date <= dt_end) \
        .filter(table_option.c.id_underlying == id_underlying) \
        .group_by(table_option.c.dt_date, table_option.c.amt_strike, table_option.c.cd_option_type)
    df = pd.read_sql(query.statement, query.session.bind)
    return df


def get_volume_groupby_id_type(table_option, namecode, dt_start, dt_end):
    query = admin.session_mktdata().query(table_option.c.dt_date, table_option.c.cd_option_type,
                                          table_option.c.id_underlying,
                                          func.sum(table_option.c.amt_holding_volume).label('total_holding_volume'),
                                          func.sum(table_option.c.amt_trading_volume).label('total_trading_volume')
                                          ) \
        .filter(table_option.c.dt_date >= dt_start).filter(table_option.c.dt_date <= dt_end) \
        .filter(table_option.c.name_code == namecode) \
        .group_by(table_option.c.cd_option_type, table_option.c.dt_date, table_option.c.id_underlying)
    df = pd.read_sql(query.statement, query.session.bind)
    return df


def get_volume_groupby_id_option(table_option, namecode, dt_start, dt_end):
    query = admin.session_mktdata().query(table_option.c.dt_date, table_option.c.id_underlying,
                                          func.sum(table_option.c.amt_holding_volume).label('total_holding_volume'),
                                          func.sum(table_option.c.amt_trading_volume).label('total_trading_volume')
                                          ) \
        .filter(table_option.c.dt_date >= dt_start).filter(table_option.c.dt_date <= dt_end) \
        .filter(table_option.c.name_code == namecode) \
        .group_by(table_option.c.dt_date,table_option.c.id_underlying)
    df = pd.read_sql(query.statement, query.session.bind)
    return df


def get_volume_groupby_id_future(table_future, namecode, dt_start, dt_end):
    query = admin.session_mktdata().query(table_future.c.dt_date, table_future.c.id_instrument,
                                          func.sum(table_future.c.amt_holding_volume).label('total_holding_volume'),
                                          func.sum(table_future.c.amt_trading_volume).label('total_trading_volume')
                                          ) \
        .filter(table_future.c.dt_date >= dt_start).filter(table_future.c.dt_date <= dt_end) \
        .filter(table_future.c.name_code == namecode) \
        .group_by(table_future.c.dt_date, table_future.c.id_instrument)
    df = pd.read_sql(query.statement, query.session.bind)
    return df


def iv_at_the_money(dt_date, dt_yesterday, id_underlying, df_srf):
    optionMetrics = dbt.OptionMetrics
    query_sro = admin.session_metrics().query(optionMetrics.dt_date, optionMetrics.id_instrument,
                                              optionMetrics.id_underlying,
                                              optionMetrics.amt_strike,
                                              optionMetrics.cd_option_type, optionMetrics.pct_implied_vol) \
        .filter(optionMetrics.id_underlying == id_underlying).filter(optionMetrics.dt_date >= dt_yesterday) \
        .filter(optionMetrics.dt_date <= dt_date)
    df_sro = pd.read_sql(query_sro.statement, query_sro.session.bind)
    dates = df_sro['dt_date'].unique()
    dict_iv_call = {}
    dict_iv_put = {}
    for date in dates:
        df0 = df_sro[df_sro['dt_date'] == date]
        df1 = df0[(df0['cd_option_type'] == 'call')]
        amt_settle = \
            df_srf[(df_srf['dt_date'] == date) & (df_srf['id_instrument'] == id_underlying)]['amt_settlement'].values[0]
        df1['diff'] = abs(df1['amt_strike'] - amt_settle)
        df1 = df1.sort_values(by='diff', ascending=True)
        k = df1.iloc[0]['amt_strike']
        iv_call = df1.iloc[0]['pct_implied_vol'] * 100
        iv_put = df0[(df0['cd_option_type'] == 'put') & (df0['amt_strike'] == k)]['pct_implied_vol'].values[0] * 100
        dict_iv_call.update({date: iv_call})
        dict_iv_put.update({date: iv_put})
    return dict_iv_call, dict_iv_put

""" get c1 by highest trading volume every day. """
def df_iv_at_the_money(dt_date, dt_start, namecode, df_srf):
    optionMetrics = dbt.OptionMetrics
    query_sro = admin.session_metrics().query(optionMetrics.dt_date, optionMetrics.id_instrument,
                                              optionMetrics.id_underlying,
                                              optionMetrics.amt_strike,
                                              optionMetrics.cd_option_type, optionMetrics.pct_implied_vol) \
        .filter(optionMetrics.dt_date >= dt_start) \
        .filter(optionMetrics.dt_date <= dt_date)\
        .filter(optionMetrics.name_code == namecode)
    df_sro = pd.read_sql(query_sro.statement, query_sro.session.bind)
    dates = df_sro['dt_date'].unique()
    dict_iv_call = []
    dict_iv_put = []
    for date in dates:
        df_volume_groupby = get_volume_groupby_id_option(admin.table_options_mktdata(), namecode, dt_start=date,
                                                         dt_end=date). \
            sort_values(by='total_trading_volume', ascending=False).reset_index(drop=True)
        id_c1 = df_volume_groupby.loc[0, 'id_underlying']
        df0 = df_sro[(df_sro['dt_date'] == date) & (df_sro['id_underlying'] == id_c1)]
        df1 = df0[(df0['cd_option_type'] == 'call')]
        amt_settle = \
            df_srf[(df_srf['dt_date'] == date) & (df_srf['id_instrument'] == id_c1)]['amt_settlement'].values[0]
        df1['diff'] = abs(df1['amt_strike'] - amt_settle)
        df1 = df1.sort_values(by='diff', ascending=True)
        k = df1.iloc[0]['amt_strike']
        iv_call_c1 = df1.iloc[0]['pct_implied_vol'] * 100
        iv_put_c1 = df0[(df0['cd_option_type'] == 'put') & (df0['amt_strike'] == k)]['pct_implied_vol'].values[0] * 100
        id_c2 = df_volume_groupby.loc[1, 'id_underlying']
        df0 = df_sro[(df_sro['dt_date'] == date) & (df_sro['id_underlying'] == id_c2)]
        df1 = df0[(df0['cd_option_type'] == 'call')]
        amt_settle = \
            df_srf[(df_srf['dt_date'] == date) & (df_srf['id_instrument'] == id_c1)]['amt_settlement'].values[0]
        df1['diff'] = abs(df1['amt_strike'] - amt_settle)
        df1 = df1.sort_values(by='diff', ascending=True)
        k = df1.iloc[0]['amt_strike']
        iv_call_c2 = df1.iloc[0]['pct_implied_vol'] * 100
        iv_put_c2 = df0[(df0['cd_option_type'] == 'put') & (df0['amt_strike'] == k)]['pct_implied_vol'].values[0] * 100
        dict_iv_call.append({'dt_date': date,
                             'iv_c1': iv_call_c1,
                             'iv_c2': iv_call_c2,
                             'underlying_c1': id_c1,
                             'underlying_c2': id_c2,
                             })
        dict_iv_put.append({'dt_date': date,
                             'iv_c1': iv_put_c1,
                             'iv_c2': iv_put_c2,
                             'underlying_c1': id_c1,
                             'underlying_c2': id_c2,
                            })
    df_call = pd.DataFrame(dict_iv_call)
    df_put = pd.DataFrame(dict_iv_put)
    return df_call, df_put

"""  Given Underlying id """
def df_iv_at_the_money_1(dt_date, dt_start, id_c1, df_srf):
    optionMetrics = dbt.OptionMetrics
    query_sro = admin.session_metrics().query(optionMetrics.dt_date, optionMetrics.id_instrument,
                                              optionMetrics.id_underlying,
                                              optionMetrics.amt_strike,
                                              optionMetrics.cd_option_type, optionMetrics.pct_implied_vol) \
        .filter(optionMetrics.dt_date >= dt_start) \
        .filter(optionMetrics.dt_date <= dt_date)\
        .filter(optionMetrics.id_underlying == id_c1)
    df_sro = pd.read_sql(query_sro.statement, query_sro.session.bind)
    dates = df_sro['dt_date'].unique()
    dict_iv_call = []
    dict_iv_put = []
    for date in dates:
        df0 = df_sro[df_sro['dt_date'] == date]
        df1 = df0[(df0['cd_option_type'] == 'call')]
        amt_settle = \
            df_srf[(df_srf['dt_date'] == date) & (df_srf['id_instrument'] == id_c1)]['amt_settlement'].values[0]
        df1['diff'] = abs(df1['amt_strike'] - amt_settle)
        df1 = df1.sort_values(by='diff', ascending=True)
        k = df1.iloc[0]['amt_strike']
        iv_call = df1.iloc[0]['pct_implied_vol'] * 100
        iv_put = df0[(df0['cd_option_type'] == 'put') & (df0['amt_strike'] == k)]['pct_implied_vol'].values[0] * 100
        dict_iv_call.append({'dt_date': date,
                             'iv': iv_call,
                             'id_underlying': id_c1})
        dict_iv_put.append({'dt_date': date,
                            'iv': iv_put,
                            'id_underlying': id_c1})
    df_call = pd.DataFrame(dict_iv_call)
    df_put = pd.DataFrame(dict_iv_put)
    return df_call, df_put


def fun_report_compare(d1, d2):
    if d1 > d2:
        return '增加'
    elif d1 < d2:
        return '减少'
    else:
        return '持平'


def fun_report_compare2(d1, d2):
    if abs(d1 - d2) / d2 <= 0.02:
        return '持平'
    elif d1 > d2:
        return '有所提升'
    else:
        return '有所下降'


def fun_report_compare3(d1, d2):
    if abs(d1 - d2) / d2 <= 0.02:
        return '基本持平'
    elif d1 > d2:
        return '偏高'
    else:
        return '偏低'


def fun_report_compare4(d1, d2):
    if d1 == d2:
        return '亦'
    else:
        return ''


def fun_report_namecode(namecode):
    if namecode == 'm':
        return '豆粕'
    elif namecode == 'sr':
        return '白糖'
    else:
        return 'NAN'
