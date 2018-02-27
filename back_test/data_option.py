
import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from data_access.db_tables import DataBaseTables as dbt
from back_test.bkt_util import BktUtil



def get_50option_mktdata(start_date,end_date):
    engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
    Session = sessionmaker(bind=engine)
    sess = Session()

    Index_mkt = dbt.IndexMkt
    Option_mkt = dbt.OptionMkt
    options = dbt.Options
    util = BktUtil()

    query_mkt = sess.query(Option_mkt.dt_date, Option_mkt.id_instrument, Option_mkt.code_instrument,
                           Option_mkt.amt_close, Option_mkt.amt_settlement, Option_mkt.amt_last_settlement,
                           Option_mkt.amt_trading_volume,Option_mkt.pct_implied_vol
                           ) \
        .filter(Option_mkt.dt_date >= start_date).filter(Option_mkt.dt_date <= end_date) \
        .filter(Option_mkt.datasource == 'wind')

    query_option = sess.query(options.id_instrument, options.cd_option_type, options.amt_strike,
                              options.dt_maturity, options.nbr_multiplier) \
        .filter(and_(options.dt_listed <= end_date, options.dt_maturity >= start_date))

    query_etf = sess.query(Index_mkt.dt_date, Index_mkt.amt_close) \
        .filter(Index_mkt.dt_date >= start_date).filter(Index_mkt.dt_date <= end_date) \
        .filter(Index_mkt.id_instrument == 'index_50etf')

    df_mkt = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
    df_contract = pd.read_sql(query_option.statement, query_option.session.bind)
    df_50etf = pd.read_sql(query_etf.statement, query_etf.session.bind).rename(
        columns={'amt_close': util.col_underlying_price})
    df_option = df_mkt.join(df_contract.set_index('id_instrument'), how='left', on='id_instrument')

    df_option_metrics = df_option.join(df_50etf.set_index('dt_date'), how='left', on='dt_date')
    return df_option_metrics


def get_comoption_mktdata(start_date,end_date,name_code):
    engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
    Session = sessionmaker(bind=engine)
    sess = Session()
    util = BktUtil()

    Future_mkt = dbt.FutureMkt
    Option_mkt = dbt.OptionMkt
    options = dbt.Options
    query_mkt = sess.query(Option_mkt.dt_date, Option_mkt.id_instrument,Option_mkt.id_underlying,
                           Option_mkt.code_instrument,Option_mkt.amt_close, Option_mkt.amt_settlement,
                           Option_mkt.amt_last_settlement,Option_mkt.amt_trading_volume,
                           Option_mkt.pct_implied_vol
                           ) \
        .filter(Option_mkt.dt_date >= start_date).filter(Option_mkt.dt_date <= end_date) \
        .filter(Option_mkt.name_code == name_code)

    query_option = sess.query(options.id_instrument, options.cd_option_type, options.amt_strike,
                              options.dt_maturity, options.nbr_multiplier) \
        .filter(and_(options.dt_listed <= end_date, options.dt_maturity >= start_date))

    query_srf = sess.query(Future_mkt.dt_date, Future_mkt.id_instrument.label(util.col_id_underlying),
                           Future_mkt.amt_settlement.label(util.col_underlying_price)) \
        .filter(Future_mkt.dt_date >= start_date).filter(Future_mkt.dt_date <= end_date) \
        .filter(Future_mkt.name_code == name_code)

    df_srf = pd.read_sql(query_srf.statement, query_srf.session.bind)

    df_mkt = pd.read_sql(query_mkt.statement, query_mkt.session.bind)
    df_contract = pd.read_sql(query_option.statement, query_option.session.bind)

    df_option = df_mkt.join(df_contract.set_index('id_instrument'), how='left', on='id_instrument')
    df_option_metrics = pd.merge(df_option, df_srf,how='left', on=['dt_date', 'id_underlying'], suffixes=['', '_r'])
    return df_option_metrics


