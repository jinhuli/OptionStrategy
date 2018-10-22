import datetime

import pandas as pd
from sqlalchemy import *

from Utilities import admin_util as admin
from data_access.db_tables import DataBaseTables as dbt
import back_test.model.constant as c

def commodity_option_market_overview(start_date,end_date,name_code):
    optionMkt = admin.table_options_mktdata()
    futureMkt = admin.table_futures_mktdata()
    query = admin.session_mktdata().query(optionMkt.c.dt_date,optionMkt.c.name_code,
                                              func.sum(optionMkt.c.amt_trading_volume).label('option_trading_volume'),
                                            func.sum(optionMkt.c.amt_trading_value).label('option_trading_value')
                                              ) \
        .filter(optionMkt.c.dt_date >= start_date) \
        .filter(optionMkt.c.dt_date <= end_date) \
        .filter(optionMkt.c.name_code == name_code) \
        .group_by(optionMkt.c.dt_date, optionMkt.c.name_code)
    df_option_trading = pd.read_sql(query.statement, query.session.bind)
    query_future = admin.session_mktdata().query(futureMkt.c.dt_date,futureMkt.c.name_code,
                                              func.sum(futureMkt.c.amt_trading_volume).label('future_trading_volume')
                                              ) \
        .filter(futureMkt.c.dt_date >= start_date) \
        .filter(futureMkt.c.dt_date <= end_date) \
        .filter(futureMkt.c.name_code == name_code) \
        .group_by(futureMkt.c.dt_date, futureMkt.c.name_code)
    df_future_trading = pd.read_sql(query_future.statement, query_future.session.bind)
    query_option_holding = admin.session_mktdata().query(optionMkt.c.dt_date, optionMkt.c.name_code,
                                                         func.sum(optionMkt.c.amt_holding_volume).label('option_holding_volume')) \
        .filter(optionMkt.c.dt_date >= start_date) \
        .filter(optionMkt.c.dt_date <= end_date) \
        .filter(optionMkt.c.name_code == name_code) \
        .filter(or_(optionMkt.c.flag_night == 0,optionMkt.c.flag_night==-1)) \
        .group_by(optionMkt.c.dt_date, optionMkt.c.name_code)#每日日盘收盘持仓数据
    df_option_holding = pd.read_sql(query_option_holding.statement, query_option_holding.session.bind)
    query_future_holding = admin.session_mktdata().query(futureMkt.c.dt_date,futureMkt.c.name_code,
                                                         func.sum(futureMkt.c.amt_holding_volume).label('future_holding_volume')) \
        .filter(futureMkt.c.dt_date >= start_date) \
        .filter(futureMkt.c.dt_date <= end_date) \
        .filter(futureMkt.c.name_code == name_code) \
        .filter(or_(futureMkt.c.flag_night == 0,futureMkt.c.flag_night==-1)) \
        .group_by(futureMkt.c.dt_date, futureMkt.c.name_code) #每日日盘收盘持仓数据
    df_future_holding = pd.read_sql(query_future_holding.statement, query_future_holding.session.bind)
    df = pd.merge(df_option_trading,df_future_trading[[c.Util.DT_DATE,'future_trading_volume']],on=c.Util.DT_DATE)
    df = pd.merge(df,df_option_holding,on=c.Util.DT_DATE)
    df = pd.merge(df,df_future_holding,on=c.Util.DT_DATE)
    return df

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


def get_50option_mktdata(start_date, end_date):
    Index_mkt = dbt.IndexMkt
    Option_mkt = dbt.OptionMkt
    options = dbt.Options
    util = c.Util
    query_mkt = admin.session_mktdata().query(Option_mkt.dt_date, Option_mkt.id_instrument,
                                              Option_mkt.code_instrument,
                                              Option_mkt.amt_open,
                                              Option_mkt.amt_close, Option_mkt.amt_settlement,
                                              Option_mkt.amt_last_settlement,Option_mkt.amt_trading_value,
                                              Option_mkt.amt_trading_volume, Option_mkt.amt_holding_volume,
                                              Option_mkt.pct_implied_vol
                                              ) \
        .filter(Option_mkt.dt_date >= start_date).filter(Option_mkt.dt_date <= end_date) \
        .filter(Option_mkt.datasource == 'wind').filter(Option_mkt.name_code == '50etf')
    query_option = admin.session_mktdata().query(options.id_instrument, options.cd_option_type,
                                                 options.amt_strike, options.name_contract_month,
                                                 options.dt_maturity, options.nbr_multiplier) \
        .filter(and_(options.dt_listed <= end_date, options.dt_maturity >= start_date))
    query_etf = admin.session_mktdata().query(Index_mkt.dt_date, Index_mkt.amt_close, Index_mkt.amt_open,
                                              Index_mkt.id_instrument.label(util.ID_UNDERLYING)) \
        .filter(Index_mkt.dt_date >= start_date).filter(Index_mkt.dt_date <= end_date) \
        .filter(Index_mkt.id_instrument == 'index_50etf')
    df_mkt = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
    df_contract = pd.read_sql(query_option.statement, query_option.session.bind)
    df_50etf = pd.read_sql(query_etf.statement, query_etf.session.bind).rename(
        columns={'amt_close': util.AMT_UNDERLYING_CLOSE, 'amt_open': util.AMT_UNDERLYING_OPEN_PRICE})
    df_option = df_mkt.join(df_contract.set_index('id_instrument'), how='left', on='id_instrument')
    df_option_metrics = df_option.join(df_50etf.set_index('dt_date'), how='left', on='dt_date')
    return df_option_metrics

def get_50option_intraday(start_date, end_date):
    OptionIntra = admin.table_option_mktdata_intraday_gc()
    query = admin.session_intraday().query(OptionIntra.c.dt_datetime,
                                           OptionIntra.c.dt_date,
                                           OptionIntra.c.id_instrument,
                                           OptionIntra.c.amt_close,
                                           OptionIntra.c.amt_trading_volume,
                                           OptionIntra.c.amt_trading_value) \
        .filter(OptionIntra.c.dt_date >= start_date).filter(OptionIntra.c.dt_date <= end_date)
    df = pd.read_sql(query.statement, query.session.bind)
    IndexIntra = admin.table_index_mktdata_intraday()
    query1 = admin.session_intraday().query(IndexIntra.c.dt_datetime,
                                            IndexIntra.c.dt_date,
                                            IndexIntra.c.id_instrument,
                                            IndexIntra.c.amt_close)\
        .filter(IndexIntra.c.dt_date >= start_date).filter(IndexIntra.c.dt_date <= end_date)\
        .filter(IndexIntra.c.id_instrument == c.Util.STR_INDEX_50ETF)
    df_etf = pd.read_sql(query1.statement, query1.session.bind)
    df_etf = df_etf[[c.Util.DT_DATETIME,c.Util.ID_INSTRUMENT, c.Util.AMT_CLOSE]]\
        .rename(columns={c.Util.AMT_CLOSE: c.Util.AMT_UNDERLYING_CLOSE,c.Util.ID_INSTRUMENT:c.Util.ID_UNDERLYING})
    df_option_metrics = df.join(df_etf.set_index(c.Util.DT_DATETIME), how='left', on=c.Util.DT_DATETIME)
    options = dbt.Options
    query_option = admin.session_mktdata().query(options.id_instrument, options.cd_option_type,
                                                 options.amt_strike, options.name_contract_month,
                                                 options.dt_maturity, options.nbr_multiplier) \
        .filter(and_(options.dt_listed <= end_date, options.dt_maturity >= start_date))
    df_contract = pd.read_sql(query_option.statement, query_option.session.bind)
    df_option_metrics = df_option_metrics.join(df_contract.set_index(c.Util.ID_INSTRUMENT), how='left', on=c.Util.ID_INSTRUMENT)
    return df_option_metrics


def get_50option_minute_with_underlying(start_date, end_date):
    OptionIntra = admin.table_option_mktdata_intraday()
    query1 = admin.session_intraday().query(OptionIntra.c.dt_datetime,
                                            OptionIntra.c.dt_date,
                                            OptionIntra.c.id_instrument,
                                            OptionIntra.c.amt_close,
                                            OptionIntra.c.amt_trading_volume,
                                            OptionIntra.c.amt_trading_value) \
        .filter(OptionIntra.c.dt_date >= start_date).filter(OptionIntra.c.dt_date <= end_date)
    df_option = pd.read_sql(query1.statement, query1.session.bind)
    IndexIntra = admin.table_index_mktdata_intraday()
    query = admin.session_intraday().query(IndexIntra.c.dt_datetime,
                                           IndexIntra.c.amt_close) \
        .filter(IndexIntra.c.dt_date >= start_date) \
        .filter(IndexIntra.c.dt_date <= end_date) \
        .filter(IndexIntra.c.id_instrument == 'index_50etf')
    df_index = pd.read_sql(query.statement, query.session.bind)
    options = dbt.Options
    query_option = admin.session_mktdata().query(options.id_instrument, options.cd_option_type, options.amt_strike,
                                                 options.dt_maturity, options.nbr_multiplier) \
        .filter(and_(options.dt_listed <= end_date, options.dt_maturity >= start_date))

    df_index = df_index.rename(columns={'amt_close': c.Util.AMT_UNDERLYING_CLOSE})
    df_option = df_option.join(df_index.set_index('dt_datetime'), how='left', on='dt_datetime')
    df_contract = pd.read_sql(query_option.statement, query_option.session.bind)
    df_option = df_option.join(df_contract.set_index('id_instrument'), how='left', on='id_instrument')
    return df_option


# def get_50option_metricdata(start_date, end_date):
#     Index_mkt = dbt.IndexMkt
#     Option_mkt = dbt.OptionMktGolden
#     options = dbt.Options
#     util = BktUtil()
#     query_mkt = admin.session_metrics().query(Option_mkt.dt_date, Option_mkt.id_instrument, Option_mkt.code_instrument,
#                                               Option_mkt.amt_open,
#                                               Option_mkt.amt_close, Option_mkt.amt_settlement,
#                                               Option_mkt.amt_last_settlement,
#                                               Option_mkt.amt_trading_volume, Option_mkt.pct_implied_vol,
#                                               Option_mkt.amt_afternoon_close_15min, Option_mkt.amt_afternoon_open_15min,
#                                               Option_mkt.amt_morning_close_15min, Option_mkt.amt_morning_open_15min,
#                                               Option_mkt.amt_daily_avg, Option_mkt.amt_afternoon_avg,
#                                               Option_mkt.amt_morning_avg
#                                               ) \
#         .filter(Option_mkt.dt_date >= start_date).filter(Option_mkt.dt_date <= end_date) \
#         .filter(Option_mkt.datasource == 'wind')
#     query_option = admin.session_mktdata().query(options.id_instrument, options.cd_option_type, options.amt_strike,
#                                                  options.dt_maturity, options.nbr_multiplier) \
#         .filter(and_(options.dt_listed <= end_date, options.dt_maturity >= start_date))
#     query_etf = admin.session_mktdata().query(Index_mkt.dt_date, Index_mkt.amt_close, Index_mkt.amt_open,
#                                               Index_mkt.id_instrument.label(util.col_id_underlying),
#                                               ) \
#         .filter(Index_mkt.dt_date >= start_date).filter(Index_mkt.dt_date <= end_date) \
#         .filter(Index_mkt.id_instrument == 'index_50etf')
#     df_mkt = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
#     df_contract = pd.read_sql(query_option.statement, query_option.session.bind)
#     df_50etf = pd.read_sql(query_etf.statement, query_etf.session.bind).rename(
#         columns={'amt_close': util.col_underlying_close, 'amt_open': util.col_underlying_open_price})
#     df_option = df_mkt.join(df_contract.set_index('id_instrument'), how='left', on='id_instrument')
#     df_option_metrics = df_option.join(df_50etf.set_index('dt_date'), how='left', on='dt_date')
#     return df_option_metrics


def get_comoption_mktdata(start_date, end_date, name_code):
    Future_mkt = dbt.FutureMkt
    Option_mkt = dbt.OptionMkt
    options = dbt.Options
    query_mkt = admin.session_mktdata(). \
        query(Option_mkt.dt_date, Option_mkt.id_instrument, Option_mkt.id_underlying,
              Option_mkt.code_instrument, Option_mkt.amt_close, Option_mkt.amt_open,
              Option_mkt.amt_settlement,
              Option_mkt.amt_last_settlement, Option_mkt.amt_trading_volume,
              Option_mkt.pct_implied_vol, Option_mkt.amt_holding_volume,
              Option_mkt.amt_trading_volume,
              ) \
        .filter(Option_mkt.dt_date >= start_date).filter(Option_mkt.dt_date <= end_date) \
        .filter(Option_mkt.name_code == name_code).filter(Option_mkt.flag_night != 1)

    query_option = admin.session_mktdata(). \
        query(options.id_instrument, options.cd_option_type, options.amt_strike, options.name_contract_month,
              options.dt_maturity, options.nbr_multiplier) \
        .filter(and_(options.dt_listed <= end_date, options.dt_maturity >= start_date))

    query_srf = admin.session_mktdata(). \
        query(Future_mkt.dt_date,
              Future_mkt.id_instrument.label(c.Util.ID_UNDERLYING),
              Future_mkt.amt_settlement.label(c.Util.AMT_UNDERLYING_CLOSE),
              Future_mkt.amt_open.label(c.Util.AMT_UNDERLYING_OPEN_PRICE)) \
        .filter(Future_mkt.dt_date >= start_date).filter(Future_mkt.dt_date <= end_date) \
        .filter(Future_mkt.name_code == name_code).filter(Future_mkt.flag_night != 1)

    df_srf = pd.read_sql(query_srf.statement, query_srf.session.bind)
    df_mkt = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
    df_contract = pd.read_sql(query_option.statement, query_option.session.bind)
    df_option = df_mkt.join(df_contract.set_index('id_instrument'), how='left', on='id_instrument')
    df_option_metrics = pd.merge(df_option, df_srf, how='left', on=['dt_date', 'id_underlying'], suffixes=['', '_r'])
    return df_option_metrics


def get_future_mktdata(start_date, end_date, name_code):
    Futures_mkt = dbt.FutureMkt
    Futures = dbt.Futures
    query_mkt = admin.session_mktdata().query(Futures_mkt.dt_date, Futures_mkt.id_instrument, Futures_mkt.name_code,
                                              Futures_mkt.amt_close, Futures_mkt.amt_trading_volume,Futures_mkt.amt_trading_value,
                                              Futures_mkt.amt_settlement, Futures_mkt.amt_last_close,
                                              Futures_mkt.amt_last_settlement, Futures_mkt.amt_open,
                                              Futures_mkt.amt_high, Futures_mkt.amt_low) \
        .filter(Futures_mkt.dt_date >= start_date) \
        .filter(Futures_mkt.dt_date <= end_date) \
        .filter(Futures_mkt.name_code == name_code) \
        .filter(Futures_mkt.flag_night != 1)
    query_c = admin.session_mktdata().query(Futures.dt_maturity, Futures.id_instrument) \
        .filter(Futures.name_code == name_code)
    df_mkt = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
    df_c = pd.read_sql(query_c.statement, query_c.session.bind)
    if df_c.empty:
        df = df_mkt
    else:
        df = df_mkt.join(df_c.set_index('id_instrument'), how='left', on='id_instrument')
    return df

def get_gc_future_c1_daily(start_date, end_date, name_code):
    table_cf = admin.table_futures_mktdata_gc()
    query = admin.session_gc().query(table_cf.c.dt_date, table_cf.c.id_instrument,
                                          table_cf.c.amt_open, table_cf.c.amt_close, table_cf.c.amt_high,
                                          table_cf.c.amt_low,
                                          table_cf.c.amt_trading_volume). \
        filter((table_cf.c.dt_date >= start_date) & (table_cf.c.dt_date <= end_date)). \
        filter(table_cf.c.name_code == name_code)
    df = pd.read_sql(query.statement, query.session.bind)
    df = df[df['id_instrument'].str.contains("_")]
    df = c.FutureUtil.get_futures_daily_c1(df)
    return df

# TODO: deprecated, use 'get_cf_minute'
def get_dzqh_cf_minute(start_date, end_date, name_code):
    table_cf = admin.table_cf_minute()
    query = admin.session_gc().query(table_cf.c.dt_datetime, table_cf.c.id_instrument, table_cf.c.dt_date,
                                     table_cf.c.amt_open, table_cf.c.amt_close, table_cf.c.amt_trading_volume). \
        filter(
        (table_cf.c.dt_date >= start_date) & (table_cf.c.dt_date <= end_date) & (table_cf.c.name_code == name_code))
    df = pd.read_sql(query.statement, query.session.bind)
    df = df[df['id_instrument'].str.contains("_")]
    return df

# TODO: deprecated, use 'get_cf_c1_minute'
def get_dzqh_cf_c1_minute(start_date, end_date, name_code):
    table_cf = admin.table_cf_minute()
    query = admin.session_gc().query(table_cf.c.dt_datetime, table_cf.c.id_instrument, table_cf.c.dt_date,
                                     table_cf.c.amt_open, table_cf.c.amt_close, table_cf.c.amt_trading_volume). \
        filter(
        (table_cf.c.dt_date >= start_date) & (table_cf.c.dt_date <= end_date) & (table_cf.c.name_code == name_code))
    df = pd.read_sql(query.statement, query.session.bind)
    df = df[df['id_instrument'].str.contains("_")]
    df = c.FutureUtil.get_futures_minute_c1(df)
    return df


# def get_dzqh_cf_daily(start_date, end_date, name_code):
#     table_cf = admin.table_cf_daily()
#     table_contracts = admin.table_future_contracts()
#     query = admin.session_dzqh().query(table_cf.c.dt_date, table_cf.c.id_instrument,
#                                        table_cf.c.amt_open, table_cf.c.amt_close, table_cf.c.amt_trading_volume,
#                                        table_cf.c.amt_trading_value). \
#         filter((table_cf.c.dt_date >= start_date) & (table_cf.c.dt_date <= end_date)). \
#         filter(table_cf.c.name_code == name_code)
#     df = pd.read_sql(query.statement, query.session.bind)
#     df = df[df['id_instrument'].str.contains("_")]
#     query_contracts = admin.session_mktdata().query(table_contracts.c.id_instrument, table_contracts.c.dt_maturity) \
#         .filter(table_contracts.c.name_code == name_code.upper())
#     df_contracts = pd.read_sql(query_contracts.statement, query_contracts.session.bind)
#     df_contracts.loc[:, c.Util.ID_INSTRUMENT] = df_contracts[c.Util.ID_INSTRUMENT].apply(lambda x: x.lower())
#     df = df.join(df_contracts.set_index(c.Util.ID_INSTRUMENT), on=c.Util.ID_INSTRUMENT, how='left')
#     return df
#
#
# def get_dzqh_cf_c1_daily(start_date, end_date, name_code):
#     table_cf = admin.table_cf_daily()
#     query = admin.session_dzqh().query(table_cf.c.dt_date, table_cf.c.id_instrument,
#                                        table_cf.c.amt_open, table_cf.c.amt_close, table_cf.c.amt_high,
#                                        table_cf.c.amt_low,
#                                        table_cf.c.amt_trading_volume). \
#         filter((table_cf.c.dt_date >= start_date) & (table_cf.c.dt_date <= end_date)). \
#         filter(table_cf.c.name_code == name_code)
#     df = pd.read_sql(query.statement, query.session.bind)
#     df = df[df['id_instrument'].str.contains("_")]
#     df = c.FutureUtil.get_futures_daily_c1(df)
#     return df



def get_mktdata_future_c1_daily(start_date, end_date, name_code):
    table_cf = admin.table_futures_mktdata()
    query = admin.session_mktdata().query(table_cf.c.dt_date, table_cf.c.id_instrument,
                                          table_cf.c.amt_open, table_cf.c.amt_close, table_cf.c.amt_high,
                                          table_cf.c.amt_low,
                                          table_cf.c.amt_trading_volume,table_cf.c.amt_trading_value). \
        filter((table_cf.c.dt_date >= start_date) & (table_cf.c.dt_date <= end_date)). \
        filter(table_cf.c.name_code == name_code).filter(table_cf.c.flag_night != 1)
    df = pd.read_sql(query.statement, query.session.bind)
    df = df[df['id_instrument'].str.contains("_")]
    df = c.FutureUtil.get_futures_daily_c1(df)
    return df


def get_mktdata_future_daily(start_date, end_date, name_code):
    table_cf = admin.table_futures_mktdata()
    table_contracts = admin.table_future_contracts()
    query = admin.session_mktdata().query(table_cf.c.dt_date, table_cf.c.id_instrument,
                                          table_cf.c.amt_open, table_cf.c.amt_close, table_cf.c.amt_trading_volume,
                                          table_cf.c.amt_trading_value,table_cf.c.amt_trading_value). \
        filter((table_cf.c.dt_date >= start_date) & (table_cf.c.dt_date <= end_date)). \
        filter(table_cf.c.name_code == name_code)
    df = pd.read_sql(query.statement, query.session.bind)
    df = df[df['id_instrument'].str.contains("_")]
    query_contracts = admin.session_mktdata().query(table_contracts.c.id_instrument, table_contracts.c.dt_maturity) \
        .filter(table_contracts.c.name_code == name_code.upper())
    df_contracts = pd.read_sql(query_contracts.statement, query_contracts.session.bind)
    df_contracts.loc[:, c.Util.ID_INSTRUMENT] = df_contracts[c.Util.ID_INSTRUMENT].apply(lambda x: x.lower())
    df = df.join(df_contracts.set_index(c.Util.ID_INSTRUMENT), on=c.Util.ID_INSTRUMENT, how='left')
    return df


# def get_dzqh_ih_c1_by_option_daily(start_date, end_date, min_holding):
#     table_option_contracts = admin.table_option_contracts()
#     query = admin.session_mktdata().query(table_option_contracts.c.id_underlying, table_option_contracts.c.dt_maturity)\
#                                     .filter(table_option_contracts.c.id_underlying == c.Util.STR_INDEX_50ETF)
#     df_option_maturity = pd.read_sql(query.statement, query.session.bind).drop_duplicates(c.Util.DT_MATURITY)
#     df_option_maturity[c.Util.DT_MATURITY] = df_option_maturity[c.Util.DT_MATURITY].apply(
#         lambda x: x - datetime.timedelta(days=min_holding))
#     df_future = get_dzqh_cf_daily(start_date, end_date, 'ih')
#     df_future['id_core'] = df_future[c.Util.DT_DATE].apply(lambda x: fun_get_c1_by_option(x, df_option_maturity))
#     df_future = df_future[df_future[c.Util.ID_INSTRUMENT] == df_future['id_core']].reset_index(drop=True)
#     return df_future
#
#
# def get_dzqh_ih_c1_by_option_minute(start_date, end_date,name_code, option_maturities):
#     table_cf = admin.table_cf_minute()
#     query = admin.session_gc().query(table_cf.c.dt_datetime, table_cf.c.id_instrument, table_cf.c.dt_date,
#                                        table_cf.c.amt_open, table_cf.c.amt_close, table_cf.c.amt_trading_volume). \
#         filter(
#         (table_cf.c.dt_date >= start_date) & (table_cf.c.dt_date <= end_date) & (table_cf.c.name_code == name_code))
#     df = pd.read_sql(query.statement, query.session.bind)
#     df = df[df['id_instrument'].str.contains("_")]
#     # TODO
#     df = c.FutureUtil.get_future_c1_by_option_mdt_minute(df, option_maturities)
#     return df

def get_cf_c1_minute(start_date, end_date, name_code):
    table_cf = admin.table_cf_minute_gc()
    query = admin.session_gc().query(table_cf.c.dt_datetime, table_cf.c.id_instrument, table_cf.c.dt_date,
                                     table_cf.c.amt_open, table_cf.c.amt_close, table_cf.c.amt_trading_volume). \
        filter(
        (table_cf.c.dt_date >= start_date) & (table_cf.c.dt_date <= end_date) & (table_cf.c.name_code == name_code))
    df = pd.read_sql(query.statement, query.session.bind)
    df = df[df['id_instrument'].str.contains("_")]
    df = c.FutureUtil.get_futures_minute_c1(df)
    return df


""" 基于商品期权到期日，构建标的期货c1时间序列 """


def get_future_c1_by_option_daily(start_date, end_date, name_code, min_holding):
    table_option_contracts = admin.table_option_contracts()
    query = admin.session_mktdata().query(table_option_contracts.c.id_underlying, table_option_contracts.c.dt_maturity)
    df_option_maturity = pd.read_sql(query.statement, query.session.bind).drop_duplicates(c.Util.DT_MATURITY)
    for id_underlying in c.OptionFilter.dict_maturities.keys():
        if id_underlying not in df_option_maturity[c.Util.ID_UNDERLYING]:
            df_option_maturity = df_option_maturity.append(
                {c.Util.ID_UNDERLYING: id_underlying, c.Util.DT_MATURITY: c.OptionFilter.dict_maturities[id_underlying]}
                , ignore_index=True)
    df_option_maturity['is_core'] = df_option_maturity[c.Util.ID_UNDERLYING].apply(
        lambda x: True if (x[-2:] in c.Util.MAIN_CONTRACT_159) and (x.split('_')[0] == name_code) else False)
    df_option_maturity = df_option_maturity[df_option_maturity['is_core']]
    df_option_maturity[c.Util.DT_MATURITY] = df_option_maturity[c.Util.DT_MATURITY].apply(
        lambda x: x - datetime.timedelta(days=min_holding))
    df_future = get_future_mktdata(start_date, end_date, name_code)
    df_future['id_core'] = df_future[c.Util.DT_DATE].apply(lambda x: fun_get_c1_by_option(x, df_option_maturity))
    df_future = df_future[df_future[c.Util.ID_INSTRUMENT] == df_future['id_core']].reset_index(drop=True)
    return df_future


def fun_get_c1_by_option(dt_date, df_option_maturity):
    df_option_maturity = df_option_maturity.sort_values(c.Util.DT_MATURITY, ascending=True).reset_index(drop=True)
    df = df_option_maturity[df_option_maturity[c.Util.DT_MATURITY] >= dt_date]
    id_core = df.iloc[0][c.Util.ID_UNDERLYING]
    return id_core


def get_iv_by_moneyness(start_date, end_date, name_code, nbr_moneyness=0,
                        cd_mdt_selection='hp_8_1st', cd_atm_criterion='nearest_strike'):
    table_iv = admin.table_implied_volatilities()
    query = admin.session_metrics().query(table_iv).filter(table_iv.c.dt_date >= start_date) \
        .filter(table_iv.c.dt_date <= end_date) \
        .filter(table_iv.c.name_code == name_code) \
        .filter(table_iv.c.nbr_moneyness == nbr_moneyness) \
        .filter(table_iv.c.cd_mdt_selection == cd_mdt_selection) \
        .filter(table_iv.c.cd_atm_criterion == cd_atm_criterion)
    df = pd.read_sql(query.statement, query.session.bind)
    return df


def get_50etf_mktdata(start_date, end_date):
    Index_mkt = dbt.IndexMkt
    query_etf = admin.session_mktdata().query(Index_mkt.dt_date, Index_mkt.amt_close, Index_mkt.id_instrument) \
        .filter(Index_mkt.dt_date >= start_date).filter(Index_mkt.dt_date <= end_date) \
        .filter(Index_mkt.id_instrument == 'index_50etf')
    df = pd.read_sql(query_etf.statement, query_etf.session.bind)
    return df


def get_index_mktdata(start_date, end_date, id_index):
    Index_mkt = admin.table_indexes_mktdata()
    query_etf = admin.session_mktdata().query(Index_mkt.c.dt_date, Index_mkt.c.amt_close, Index_mkt.c.amt_open,
                                              Index_mkt.c.id_instrument, Index_mkt.c.amt_high, Index_mkt.c.amt_low) \
        .filter(Index_mkt.c.dt_date >= start_date).filter(Index_mkt.c.dt_date <= end_date) \
        .filter(Index_mkt.c.id_instrument == id_index)
    df_index = pd.read_sql(query_etf.statement, query_etf.session.bind)
    return df_index


def get_index_intraday(start_date, end_date, id_index):
    Index = admin.table_index_mktdata_intraday()
    query = admin.session_intraday().query(Index.c.dt_datetime,Index.c.dt_date, Index.c.id_instrument, Index.c.amt_close,
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


def get_vix(start_date, end_date):
    df1 = get_index_mktdata(start_date, datetime.date(2017, 6, 1), 'index_cvix')[
        ['dt_date', 'amt_close', 'id_instrument']]
    xl = pd.ExcelFile('../data/VIX_daily.xlsx')
    df2 = xl.parse("Sheet1", header=None)
    df2['dt_date'] = df2[0]
    df2['amt_close'] = df2[1]
    df2 = df2[['dt_date', 'amt_close']]
    df2['dt_date'] = df2['dt_date'].apply(lambda x: x.date())
    df2 = df2[(df2['dt_date'] > datetime.date(2017, 6, 1)) & (df2['dt_date'] <= end_date)]
    df2['id_instrument'] = 'index_cvix'
    df = pd.concat([df1, df2]).sort_values(by='dt_date').reset_index(drop=True)
    df['amt_close'] = df['amt_close'] / 100.0
    return df
