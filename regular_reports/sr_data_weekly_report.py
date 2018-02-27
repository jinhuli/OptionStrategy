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
from regular_reports.sr_option_week_report import sr_hist_atm_ivs,sr_implied_vol_analysis,sr_pcr_analysis

############################################################################################
# Eval Settings

dt_date = datetime.date(2018, 2, 23)  # Set as Friday
dt_last_week = datetime.date(2018, 2, 9)
current_core_underlying = 'sr_1805'

############################################################################################
w.start()
endDate = dt_date
evalDate = endDate.strftime("%Y-%m-%d")  # Set as Friday
startDate = datetime.date(2017, 4, 19)
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

######################################## PART 1 : 标的历史波动率 #####################################################
"""读取历史已实现波动率：1M、2M、3M、6M"""
query_srf = sess2.query(futureMkt.dt_date, futureMkt.id_instrument,
                        futureMkt.amt_close, futureMkt.amt_trading_volume,
                        futureMkt.amt_settlement) \
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
df_core.to_csv('../save_results/sr_future_hist_vols.csv')
print('part1 completed')
######################################## PART 2 : 成交持仓认沽认购比 #####################################################
"""成交持仓认沽认购比P/C"""
query_volume = sess2.query(optionMkt.dt_date, optionMkt.cd_option_type,
                           func.sum(optionMkt.amt_holding_volume).label('total_holding_volume'),
                           func.sum(optionMkt.amt_trading_volume).label('total_trading_volume')
                           ) \
    .filter(optionMkt.dt_date >= startDate) \
    .filter(optionMkt.name_code == 'sr') \
    .filter(optionMkt.id_underlying == current_core_underlying) \
    .group_by(optionMkt.cd_option_type, optionMkt.dt_date)

df = pd.read_sql(query_volume.statement, query_volume.session.bind)

df_call = df[df['cd_option_type'] == 'call'].reset_index()
df_put = df[df['cd_option_type'] == 'put'].reset_index()
pc_ratio = []
for idx, row in df_call.iterrows():
    row_put = df_put[df_put['dt_date'] == row['dt_date']]
    pcr_trading = row_put['total_trading_volume'].values[0] / row['total_trading_volume']
    pcr_holding = row_put['total_holding_volume'].values[0] / row['total_holding_volume']
    pc_ratio.append({'dt_date': row['dt_date'],
                     '2成交量-C': row['total_trading_volume'],
                     '3成交量-P': row_put['total_trading_volume'].values[0],
                     '4持仓量-C': row['total_holding_volume'],
                     '5持仓量-P': row_put['total_holding_volume'].values[0],
                     '6持仓量PCR': pcr_holding,
                     '7成交量PCR': pcr_trading
                     })

df_pcr = pd.DataFrame(pc_ratio)
df_pcr = pd.merge(df_pcr,df_srf[['dt_date','amt_settlement']], how='left', on=['dt_date'], suffixes=['', '_r'])
df_pcr = df_pcr.sort_values(by='dt_date',ascending=False)

df_pcr.to_csv('../save_results/sr_pcr_data.csv')
print('part2 completed')

######################################## PART 3 : 调取周报程序 #####################################################

sr_implied_vol_analysis(evalDate,w)
sr_hist_atm_ivs(evalDate,w)

print('part3 completed')























