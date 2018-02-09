from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import datetime
import pandas as pd
import numpy as np
from WindPy import w
from data_access.db_tables import DataBaseTables as dbt
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil

#############################################################################################

w.start()
pu = PlotUtil()
engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata',
                       echo=False)
conn = engine.connect()
metadata = MetaData(engine)
Session = sessionmaker(bind=engine)
sess = Session()
options_mkt = Table('options_mktdata', metadata, autoload=True)
futures_mkt = Table('futures_mktdata', metadata, autoload=True)
options = Table('option_contracts', metadata, autoload=True)
futuremkt_table = dbt.FutureMkt
optionmkt_table = dbt.OptionMkt
options_table = dbt.Options

# Eval Settings
evalDate = datetime.date(2018, 2, 2).strftime("%Y-%m-%d")  # Set as Friday
startDate = datetime.date(2017, 4, 19).strftime("%Y-%m-%d")

plt.rcParams['font.sans-serif'] = ['STKaiti']
plt.rcParams.update({'font.size': 15})
flagNight = 0
nameCode = 'sr'
contracts = ['1705','1709','1801','1805', '1809','1901','1905']


#############################################################################################
"""读取ATM隐含波动率"""
query_sro = sess.query(optionmkt_table.dt_date,optionmkt_table.id_instrument,optionmkt_table.id_underlying,
                       optionmkt_table.amt_strike,optionmkt_table.cd_option_type,optionmkt_table.pct_implied_vol)\
    .filter(optionmkt_table.dt_date >= startDate).filter(optionmkt_table.name_code == 'sr')\
    .filter(optionmkt_table.datasource=='czce')

query_mdt = sess.query(options_table.id_instrument,options_table.id_underlying,options_table.dt_maturity)\
    .filter(options_table.cd_exchange=='czce')

query_srf = sess.query(futuremkt_table.dt_date, futuremkt_table.id_instrument,
                       futuremkt_table.amt_close, futuremkt_table.amt_trading_volume,futuremkt_table.amt_settlement) \
    .filter(futuremkt_table.dt_date >= startDate).filter(futuremkt_table.name_code == 'sr')

df_srf = pd.read_sql(query_srf.statement, query_srf.session.bind)
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
        id_instrument = df2['id_instrument'].values[0]
        amt_settle = df_srf[(df_srf['dt_date']==date)&(df_srf['id_instrument']==underlying)]['amt_settlement'].values[0]
        try:
            mdt = df_mdt[df_mdt['id_instrument']==id_instrument]['dt_maturity'].values[0]
        except:
            m1 = int(underlying[-2:])
            y1 = int(str(20)+underlying[-4:-2])
            dt1 = datetime.date(y1,m1,1)
            mdt = w.tdaysoffset(-5, dt1, "Period=D").Data[0][0].date()
        ttm = (mdt-date).days/365.0
        df2['diff'] = abs(df2['amt_strike']-amt_settle)
        df2 = df2.sort_values(by='diff',ascending=True)
        df_atm = df2[0:1]
        df_atm['ttm'] = ttm
        df_iv_atm = df_iv_atm.append(df_atm,ignore_index=True)

df_iv_results = pd.DataFrame()
dates = df_sro['dt_date'].unique()
for idx_dt,date in enumerate(dates):
    df0 = df_iv_atm[df_iv_atm['dt_date'] == date].reset_index()
    df_iv_results.loc[idx_dt,'dt_date'] = date
    for i in range(len(df0)):
        df_iv_results.loc[idx_dt,'contract-'+str(i+1)] = df0.loc[i,'pct_implied_vol']
        df_iv_results.loc[idx_dt,'maturity-'+str(i+1)] = df0.loc[i,'ttm']

df_iv_results = df_iv_results.sort_values(by='dt_date',ascending=False)
print(df_iv_results)
# df_iv_results.to_csv('../save_results/sr_implied_vols.csv')

core_ivs = df_iv_results['contract-1'].tolist()
current_iv = core_ivs[0]
p_75 = np.percentile(core_ivs,75)*0.01
p_25 = np.percentile(core_ivs,25)*0.01

current_iv_pct = 0
diff_min = 10000.0
for i in range(0,100):
    p = np.percentile(core_ivs,i)
    diff = abs(p-current_iv)
    if diff < diff_min :
        diff_min = diff
        current_iv_pct = p
print(current_iv_pct)
f1, ax1 = plt.subplots()

pu.plot_line(ax1, 0, df_iv_results['dt_date'], core_ivs, '隐含波动率', '日期', '(%)')
pu.plot_line(ax1, 1, df_iv_results['dt_date'], p_75*range(len(core_ivs)), '75分位数', '日期', '(%)')
pu.plot_line(ax1, 2, df_iv_results['dt_date'], p_25*range(len(core_ivs)), '25分位数', '日期', '(%)')

ax1.legend(bbox_to_anchor=(0., 1.02, 1., .202), loc=3,
           ncol=1, mode="expand", borderaxespad=0.,frameon=False)
f1.set_size_inches((12,6))

f1.savefig('../save_figure/sr_atm_implied_vols_' + str(evalDate) + '.png', dpi=300, format='png')




