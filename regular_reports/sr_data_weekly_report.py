from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from mpl_toolkits.mplot3d import Axes3D
import matplotlib as mpl
from matplotlib import cm as plt_cm
import datetime
import pandas as pd
import numpy as np
from WindPy import w
from data_access.db_tables import DataBaseTables as dbt
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil
import QuantLib as ql

############################################################################################
# Eval Settings
w.start()
current_core_underlying = 'sr_1805'
startDate = datetime.date(2017, 4, 19)
endDate = datetime.date(2018, 1, 26)
hist_date = w.tdaysoffset(-7, startDate, "Period=M").Data[0][0].date()
bd_1m = 21
bd_2m = 2 * bd_1m
bd_3m = 3 * bd_1m
bd_6m = 6 * bd_1m
calendar = ql.China()
###########################################################################################
engine2 = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
metadata2 = MetaData(engine2)
Session2 = sessionmaker(bind=engine2)
sess2 = Session2()
futureMkt = dbt.FutureMkt
optionMkt = dbt.OptionMkt

futuremkt_table = dbt.FutureMkt
options_table = dbt.Options

######################################## PART 1 #####################################################
"""读取历史已实现波动率：1M、2M、3M、6M"""
query_srf = sess2.query(futureMkt.dt_date, futureMkt.id_instrument,
                        futureMkt.amt_close, futureMkt.amt_trading_volume,futureMkt.amt_settlement) \
    .filter(futureMkt.dt_date >= hist_date).filter(futureMkt.name_code == 'sr')

df_srf = pd.read_sql(query_srf.statement, query_srf.session.bind)

dates = df_srf['dt_date'].unique()
df_core = pd.DataFrame()
for idx, date in enumerate(dates):
    df1 = df_srf[df_srf['dt_date'] == date]
    df1 = df1.sort_values(by='amt_trading_volume', ascending=False)
    close = df1['amt_close'].values[0]
    id_ins = df1['id_instrument'].values[0]
    df_core.loc[idx, 'dt_date'] = date
    df_core.loc[idx, 'id_core'] = id_ins
    df_core.loc[idx, 'amt_close'] = close

for (idx, row) in df_core.iterrows():
    if idx == 0: r = 0.0
    else:
        r = np.log(float(row['amt_close']) / float(df_core.loc[idx - 1, 'amt_close']))
        df_core.loc[idx, 'yield'] = r

for idx_mkt in range(len(df_core)):
    if idx_mkt >= bd_6m:
        df_core.loc[idx_mkt, '5近半年'] = np.std(df_core['yield'][idx_mkt-bd_6m:idx_mkt])*np.sqrt(252)*100
    if idx_mkt >= bd_3m:
        df_core.loc[idx_mkt, '4近三月'] = np.std(df_core['yield'][idx_mkt-bd_3m:idx_mkt])*np.sqrt(252)*100
    if idx_mkt >= bd_2m:
        df_core.loc[idx_mkt, '3近两月'] = np.std(df_core['yield'][idx_mkt-bd_2m:idx_mkt])*np.sqrt(252)*100
    if idx_mkt >= bd_1m:
        df_core.loc[idx_mkt, '2近一月'] = np.std(df_core['yield'][idx_mkt-bd_1m:idx_mkt])*np.sqrt(252)*100

df_core = df_core[df_core['dt_date']>=startDate]
df_core = df_core[['dt_date','2近一月','3近两月','4近三月','5近半年']]
df_core = df_core.sort_values(by='dt_date',ascending=False)
df_core.to_csv('../save_results/sr_hist_vols.csv')

######################################### PART 2 ####################################################
"""读取ATM隐含波动率"""
query_sro = sess2.query(optionMkt.dt_date,optionMkt.id_underlying,optionMkt.amt_strike,
                        optionMkt.cd_option_type,optionMkt.pct_implied_vol)\
    .filter(optionMkt.dt_date >= startDate).filter(optionMkt.name_code == 'sr')\
    .filter(optionMkt.datasource=='czce')
query_mdt = sess2.query(options_table.id_underlying,options_table.dt_maturity)\
    .filter(options_table.cd_exchange=='czce')

df_sro = pd.read_sql(query_sro.statement, query_sro.session.bind)
df_mdt = pd.read_sql(query_mdt.statement, query_mdt.session.bind)

df_iv_atm = pd.DataFrame()

dates = df_sro['dt_date'].unique()
for date in dates:
    df0 = df_sro[df_sro['dt_date'] == date]
    underlyings = df0['id_underlying'].unique()
    months = []
    for u in underlyings:
        months.append(u[-4:])
    months = sorted(months)
    core = ['01','05','09']
    underlyings_core = []
    for m in months:
        if m[-2:] in core:
            underlyings_core.append(m)
            core.remove(m[-2:])
    for underlying in underlyings:
        if underlying[-4:] not in underlyings_core:continue
        df1 = df0[df0['cd_option_type']=='call']
        df2 = df1[df1['id_underlying']==underlying]
        amt_settle = df_srf[(df_srf['dt_date']==date)&(df_srf['id_instrument']==underlying)]['amt_settlement'].values[0]
        df2['diff'] = abs(df2['amt_strike']-amt_settle)
        df2 = df2.sort_values(by='diff',ascending=True)
        df_atm = df2[0:1]
        df_iv_atm = df_iv_atm.append(df_atm,ignore_index=True)


df_iv_results = pd.DataFrame()
dates = df_sro['dt_date'].unique()
for idx_dt,date in enumerate(dates):
    df0 = df_iv_atm[df_iv_atm['dt_date'] == date].reset_index()
    df_iv_results.loc[idx_dt,'dt_date'] = date
    for i in range(len(df0)):
        df_iv_results.loc[idx_dt,'contract-'+str(i+1)] = df0.loc[i,'pct_implied_vol']

df_iv_results = df_iv_results.sort_values(by='dt_date',ascending=False)
print(df_iv_results)
df_iv_results.to_csv('../save_results/sr_implied_vols.csv')


######################################### PART 3 ####################################################
"""成交持仓认沽认购比P/C"""
query_volume = sess2.query(optionMkt.dt_date, optionMkt.cd_option_type,
                           func.sum(optionMkt.amt_holding_volume).label('total_holding_volume'),
                           func.sum(optionMkt.amt_trading_volume).label('total_trading_volume')
                           ) \
    .filter(optionMkt.dt_date >= startDate) \
    .filter(optionMkt.name_code == 'sr') \
    .filter(optionMkt.id_underlying == current_core_underlying) \
    .group_by(optionMkt.cd_option_type, optionMkt.dt_date)

df_volume = pd.read_sql(query_volume.statement, query_volume.session.bind)

df_call = df_volume[df_volume['cd_option_type'] == 'call'].reset_index()
df_put = df_volume[df_volume['cd_option_type'] == 'put'].reset_index()
pc_ratio = []
for idx, row in df_call.iterrows():
    row_put = df_put[df_put['dt_date'] == row['dt_date']]
    if row['total_trading_volume'] == 0:
        pcr_trading = None
    else:
        pcr_trading = row_put['total_trading_volume'].values[0] / row['total_trading_volume']
    if row['total_holding_volume'] == 0:
        pcr_holding = None
    else:
        pcr_holding = row_put['total_holding_volume'].values[0] / row['total_holding_volume']
    pc_ratio.append(
        {'1dt_date': row['dt_date'],
         '2成交量-C': row['total_trading_volume'],
         '3成交量-P': row_put['total_trading_volume'].values[0],
         '4持仓量-C': row['total_holding_volume'],
         '5持仓量-P': row_put['total_holding_volume'].values[0],
         '6持仓量PCR': pcr_holding,
         '7成交量PCR': pcr_trading
         })

df_pcr = pd.DataFrame(pc_ratio)
df_pcr = df_pcr.sort_values(by='1dt_date',ascending=False)
df_pcr.to_csv('../save_results/sr_pcr.csv')

print(df_pcr)





























