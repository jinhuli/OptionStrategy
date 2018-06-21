import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from data_access.db_tables import DataBaseTables as dbt
from back_test.BktUtil import BktUtil
from Utilities import admin_util as admin


def get_eventsdata(start_date, end_date, flag_impact):
    events = admin.table_events()
    query = admin.session_mktdata().query(events.c.dt_date, events.c.id_event, events.c.name_event,
                                          events.c.cd_occurrence,
                                          events.c.dt_impact_beg,
                                          events.c.cd_trade_direction, events.c.dt_test, events.c.dt_test2,
                                          events.c.dt_impact_end, events.c.dt_vol_peak, events.c.cd_open_position_time,
                                          events.c.cd_close_position_time) \
        .filter(events.c.dt_date >= start_date) \
        .filter(events.c.dt_date <= end_date) \
        .filter(events.c.flag_impact == flag_impact) \
        # .filter(events.c.cd_occurrence == 'e')
    df_event = pd.read_sql(query.statement, query.session.bind)
    return df_event


def get_50etf_mktdata(start_date, end_date):
    Index_mkt = dbt.IndexMkt
    query_etf = admin.session_mktdata().query(Index_mkt.dt_date, Index_mkt.amt_close, Index_mkt.id_instrument) \
        .filter(Index_mkt.dt_date >= start_date).filter(Index_mkt.dt_date <= end_date) \
        .filter(Index_mkt.id_instrument == 'index_50etf')
    df = pd.read_sql(query_etf.statement, query_etf.session.bind)
    return df


def get_50option_mktdata(start_date, end_date):
    Index_mkt = dbt.IndexMkt
    Option_mkt = dbt.OptionMkt
    options = dbt.Options
    util = BktUtil()
    query_mkt = admin.session_mktdata().query(Option_mkt.dt_date, Option_mkt.id_instrument,
                                              Option_mkt.code_instrument,
                                              Option_mkt.amt_open,
                                              Option_mkt.amt_close, Option_mkt.amt_settlement,
                                              Option_mkt.amt_last_settlement,
                                              Option_mkt.amt_trading_volume, Option_mkt.amt_holding_volume,
                                              Option_mkt.pct_implied_vol
                                              ) \
        .filter(Option_mkt.dt_date >= start_date).filter(Option_mkt.dt_date <= end_date) \
        .filter(Option_mkt.datasource == 'wind')
    query_option = admin.session_mktdata().query(options.id_instrument, options.cd_option_type,
                                                 options.amt_strike, options.name_contract_month,
                                                 options.dt_maturity, options.nbr_multiplier) \
        .filter(and_(options.dt_listed <= end_date, options.dt_maturity >= start_date))
    query_etf = admin.session_mktdata().query(Index_mkt.dt_date, Index_mkt.amt_close, Index_mkt.amt_open,
                                              Index_mkt.id_instrument.label(util.col_id_underlying)) \
        .filter(Index_mkt.dt_date >= start_date).filter(Index_mkt.dt_date <= end_date) \
        .filter(Index_mkt.id_instrument == 'index_50etf')
    df_mkt = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
    df_contract = pd.read_sql(query_option.statement, query_option.session.bind)
    df_50etf = pd.read_sql(query_etf.statement, query_etf.session.bind).rename(
        columns={'amt_close': util.col_underlying_close, 'amt_open': util.col_underlying_open_price})
    df_option = df_mkt.join(df_contract.set_index('id_instrument'), how='left', on='id_instrument')
    df_option_metrics = df_option.join(df_50etf.set_index('dt_date'), how='left', on='dt_date')
    return df_option_metrics


def get_future_mktdata(start_date, end_date, name_code):
    Futures_mkt = dbt.FutureMkt
    Futures = dbt.Futures
    query_mkt = admin.session_mktdata().query(Futures_mkt.dt_date, Futures_mkt.id_instrument, Futures_mkt.name_code,
                                              Futures_mkt.amt_close, Futures_mkt.amt_trading_volume,
                                              Futures_mkt.amt_settlement) \
        .filter(Futures_mkt.dt_date >= start_date) \
        .filter(Futures_mkt.dt_date <= end_date) \
        .filter(Futures_mkt.name_code == name_code) \
        .filter(Futures_mkt.datasource == 'wind')
    query_c = admin.session_mktdata().query(Futures.dt_maturity, Futures.id_instrument) \
        .filter(Futures.name_code == name_code)
    df_mkt = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
    df_c = pd.read_sql(query_c.statement, query_c.session.bind)
    df = df_mkt.join(df_c.set_index('id_instrument'), how='left', on='id_instrument')
    return df


def get_50option_metricdata(start_date, end_date):
    Index_mkt = dbt.IndexMkt
    Option_mkt = dbt.OptionMktGolden
    options = dbt.Options
    util = BktUtil()
    query_mkt = admin.session_metrics().query(Option_mkt.dt_date, Option_mkt.id_instrument, Option_mkt.code_instrument,
                                              Option_mkt.amt_open,
                                              Option_mkt.amt_close, Option_mkt.amt_settlement,
                                              Option_mkt.amt_last_settlement,
                                              Option_mkt.amt_trading_volume, Option_mkt.pct_implied_vol,
                                              Option_mkt.amt_afternoon_close_15min, Option_mkt.amt_afternoon_open_15min,
                                              Option_mkt.amt_morning_close_15min, Option_mkt.amt_morning_open_15min,
                                              Option_mkt.amt_daily_avg, Option_mkt.amt_afternoon_avg,
                                              Option_mkt.amt_morning_avg
                                              ) \
        .filter(Option_mkt.dt_date >= start_date).filter(Option_mkt.dt_date <= end_date) \
        .filter(Option_mkt.datasource == 'wind')
    query_option = admin.session_mktdata().query(options.id_instrument, options.cd_option_type, options.amt_strike,
                                                 options.dt_maturity, options.nbr_multiplier) \
        .filter(and_(options.dt_listed <= end_date, options.dt_maturity >= start_date))
    query_etf = admin.session_mktdata().query(Index_mkt.dt_date, Index_mkt.amt_close, Index_mkt.amt_open,
                                              Index_mkt.id_instrument.label(util.col_id_underlying),
                                              ) \
        .filter(Index_mkt.dt_date >= start_date).filter(Index_mkt.dt_date <= end_date) \
        .filter(Index_mkt.id_instrument == 'index_50etf')
    df_mkt = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
    df_contract = pd.read_sql(query_option.statement, query_option.session.bind)
    df_50etf = pd.read_sql(query_etf.statement, query_etf.session.bind).rename(
        columns={'amt_close': util.col_underlying_close, 'amt_open': util.col_underlying_open_price})
    df_option = df_mkt.join(df_contract.set_index('id_instrument'), how='left', on='id_instrument')
    df_option_metrics = df_option.join(df_50etf.set_index('dt_date'), how='left', on='dt_date')
    return df_option_metrics


def get_index_mktdata(start_date, end_date, id_index):
    Index_mkt = admin.table_indexes_mktdata()
    query_etf = admin.session_mktdata().query(Index_mkt.c.dt_date, Index_mkt.c.amt_close, Index_mkt.c.amt_open,
                                              Index_mkt.c.id_instrument) \
        .filter(Index_mkt.c.dt_date >= start_date).filter(Index_mkt.c.dt_date <= end_date) \
        .filter(Index_mkt.c.id_instrument == id_index)
    df_index = pd.read_sql(query_etf.statement, query_etf.session.bind)
    return df_index


def get_index_intraday(start_date, end_date, id_index):
    Index = admin.table_index_mktdata_intraday()
    query = admin.session_intraday().query(Index.c.dt_datetime, Index.c.id_instrument, Index.c.amt_close,
                                           Index.c.amt_trading_volume, Index.c.amt_trading_value) \
        .filter(Index.c.dt_datetime >= start_date).filter(Index.c.dt_datetime <= end_date) \
        .filter(Index.c.id_instrument == id_index)
    df = pd.read_sql(query.statement, query.session.bind)
    return df


def get_index_ma(start_date, end_date, id_index):
    Index_mkt = admin.table_moving_average()
    query_etf = admin.session_metrics().query(Index_mkt.c.dt_date, Index_mkt.c.amt_close,
                                              Index_mkt.c.id_instrument, Index_mkt.c.amt_value, Index_mkt.c.cd_period) \
        .filter(Index_mkt.c.dt_date >= start_date).filter(Index_mkt.c.dt_date <= end_date) \
        .filter(Index_mkt.c.id_instrument == id_index)
    df_index = pd.read_sql(query_etf.statement, query_etf.session.bind)
    return df_index


def get_pciv_ratio(end_date):
    table = admin.table_option_iv_by_moneyness()
    query1 = admin.session_metrics().query(table.c.dt_date, table.c.pct_implies_vol) \
        .filter(table.c.dt_date <= end_date).filter(table.c.id_underlying == 'index_50etf') \
        .filter(table.c.cd_option_type == 'call').filter(table.c.cd_mdt == 'hp_8_1st')
    df = pd.read_sql(query1.statement, query1.session.bind)
    df = df.rename(columns={'pct_implies_vol': 'iv_call'})
    query2 = admin.session_metrics().query(table.c.dt_date, table.c.pct_implies_vol) \
        .filter(table.c.dt_date <= end_date).filter(table.c.id_underlying == 'index_50etf') \
        .filter(table.c.cd_option_type == 'put').filter(table.c.cd_mdt == 'hp_8_1st')
    df2 = pd.read_sql(query2.statement, query2.session.bind)
    df['iv_put'] = df2['pct_implies_vol']
    df['amt_close'] = (df['iv_put'] - df['iv_call']) / ((df['iv_put'] + df['iv_call']) / 2)
    # df['amt_close'] = (df['iv_put']-df['iv_call'])
    return df


def get_comoption_mktdata(start_date, end_date, name_code):
    util = BktUtil()
    Future_mkt = dbt.FutureMkt
    Option_mkt = dbt.OptionMkt
    options = dbt.Options
    query_mkt = admin.session_mktdata(). \
        query(Option_mkt.dt_date, Option_mkt.id_instrument, Option_mkt.id_underlying,
              Option_mkt.code_instrument, Option_mkt.amt_close, Option_mkt.amt_open,
              Option_mkt.amt_settlement,
              Option_mkt.amt_last_settlement, Option_mkt.amt_trading_volume,
              Option_mkt.pct_implied_vol
              ) \
        .filter(Option_mkt.dt_date >= start_date).filter(Option_mkt.dt_date <= end_date) \
        .filter(Option_mkt.name_code == name_code).filter(Option_mkt.flag_night != 1)

    query_option = admin.session_mktdata(). \
        query(options.id_instrument, options.cd_option_type, options.amt_strike,
              options.dt_maturity, options.nbr_multiplier) \
        .filter(and_(options.dt_listed <= end_date, options.dt_maturity >= start_date))

    query_srf = admin.session_mktdata(). \
        query(Future_mkt.dt_date,
              Future_mkt.id_instrument.label(util.col_id_underlying),
              Future_mkt.amt_settlement.label(util.col_underlying_close),
              Future_mkt.amt_open.label(util.col_underlying_open_price)) \
        .filter(Future_mkt.dt_date >= start_date).filter(Future_mkt.dt_date <= end_date) \
        .filter(Future_mkt.name_code == name_code).filter(Future_mkt.flag_night != 1)

    df_srf = pd.read_sql(query_srf.statement, query_srf.session.bind)
    df_mkt = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
    df_contract = pd.read_sql(query_option.statement, query_option.session.bind)
    df_option = df_mkt.join(df_contract.set_index('id_instrument'), how='left', on='id_instrument')
    df_option_metrics = pd.merge(df_option, df_srf, how='left', on=['dt_date', 'id_underlying'], suffixes=['', '_r'])
    return df_option_metrics
