from sqlalchemy import *
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


"""成交持仓认沽认购比P/C"""
def pcr(df_pcr):
    # 按期权合约持仓量最大选取主力合约
    df = df_pcr[df_pcr.groupby(['dt_date','cd_option_type'])['total_holding_volume'].transform(max) == df_pcr['total_holding_volume']]
    df_call = df[df['cd_option_type'] == 'call'].reset_index()
    df_put = df[df['cd_option_type'] == 'put'].reset_index()
    pc_ratio = []
    for idx, row in df_call.iterrows():
        row_put = df_put[df_put['dt_date'] == row['dt_date']]
        pcr_trading = row_put['total_trading_volume'].values[0] / row['total_trading_volume']
        pcr_holding = row_put['total_holding_volume'].values[0] / row['total_holding_volume']
        pc_ratio.append({'dt_date': row['dt_date'],
                         'f1成交量-C': row['total_trading_volume'],
                         'f2成交量-P': row_put['total_trading_volume'].values[0],
                         'f3持仓量-C': row['total_holding_volume'],
                         'f4持仓量-P': row_put['total_holding_volume'].values[0],
                         'f5成交量PCR': pcr_trading,
                         'f6持仓量PCR': pcr_holding,
                         'id_instrument':row['id_underlying'],
                         })

    df_pcr = pd.DataFrame(pc_ratio)
    # idx = df_srf.groupby(['dt_date'])['amt_trading_volume'].transform(max) == df_srf['amt_trading_volume']
    # df2 = df_srf[idx].sort_values(by=['dt_date'],ascending=False)# 按持仓量最大选取主力合约
    df_pcr = pd.merge(df_pcr,df_srf[['dt_date','id_instrument','amt_settlement']], how='left', on=['dt_date','id_instrument'],
                      suffixes=['', '_r'])

    #计算标的历史波动率相关数据
    df_underlying_core = pd.merge(df_pcr[['dt_date','id_instrument']],df_srf, how='left', on=['dt_date','id_instrument'],
                                  suffixes=['', '_r'])

    df_pcr = df_pcr.sort_values(by='dt_date',ascending=False)
    df_pcr.to_csv('../save_results/'+namecode+'_pcr_data.csv')
    print('part [PCR] completed')

    return df_underlying_core

"""标的已实现历史波动率"""
def hist_vol(df_underlying_core):
    """历史已实现波动率：1M、2M、3M、6M"""
    for (idx, row) in df_underlying_core.iterrows():
        if idx == 0:
            r = 0.0
        else:
            p1 = float(row['amt_close'])
            p0 = float(df_underlying_core.loc[idx-1,'amt_close'])
            if p0 == 0.0 or p1 == 0.0:
                r = 0.0
            else:
                r = (p1-p0) / p0
        df_underlying_core.loc[idx, 'yield'] = r

    for idx_mkt in range(len(df_underlying_core)):
        if idx_mkt >= bd_6m:
            df_underlying_core.loc[idx_mkt, '5近半年'] = np.std(df_underlying_core['yield'][idx_mkt-bd_6m:idx_mkt])*np.sqrt(252)*100
        if idx_mkt >= bd_3m:
            df_underlying_core.loc[idx_mkt, '4近三月'] = np.std(df_underlying_core['yield'][idx_mkt-bd_3m:idx_mkt])*np.sqrt(252)*100
        if idx_mkt >= bd_2m:
            df_underlying_core.loc[idx_mkt, '3近两月'] = np.std(df_underlying_core['yield'][idx_mkt-bd_2m:idx_mkt])*np.sqrt(252)*100
        if idx_mkt >= bd_1m:
            df_underlying_core.loc[idx_mkt, '2近一月'] = np.std(df_underlying_core['yield'][idx_mkt-bd_1m:idx_mkt])*np.sqrt(252)*100

    df_histvol = df_underlying_core[df_underlying_core['dt_date']>=startDate]
    df_histvol = df_histvol[['dt_date','2近一月','3近两月','4近三月','5近半年']]
    df_histvol = df_histvol.sort_values(by='dt_date',ascending=False)
    df_histvol.to_csv('../save_results/'+namecode+'_future_hist_vols.csv')
    print('Part [历史已实现波动率] completed')

    """历史波动率锥"""
    histvols_6 = list(df_underlying_core['5近半年'].dropna())
    histvols_3 = list(df_underlying_core['4近三月'].dropna())
    histvols_2 = list(df_underlying_core['3近两月'].dropna())
    histvols_1 = list(df_underlying_core['2近一月'].dropna())
    max_vols = [max(histvols_6), max(histvols_3), max(histvols_2), max(histvols_1)]
    min_vols = [min(histvols_6), min(histvols_3), min(histvols_2), min(histvols_1)]
    median_vols = [np.median(histvols_6), np.median(histvols_3), np.median(histvols_2),
                   np.median(histvols_1)]
    p75_vols = [np.percentile(histvols_6, 75), np.percentile(histvols_3, 75),
                np.percentile(histvols_2, 75), np.percentile(histvols_1, 75)]
    p25_vols = [np.percentile(histvols_6, 25), np.percentile(histvols_3, 25),
                np.percentile(histvols_2, 25), np.percentile(histvols_1, 25)]
    current_vols = [histvols_6[-1], histvols_3[-1], histvols_2[-1], histvols_1[-1]]
    print('current_vols : ', current_vols)
    histvolcone = [current_vols, max_vols, min_vols, median_vols, p75_vols, p25_vols]
    x = [6, 3, 2, 1]
    f2, ax2 = plt.subplots()
    ldgs = ['当前水平', '最大值', '最小值', '中位数', '75分位数', '25分位数']
    for cont2, y in enumerate(histvolcone):
        pu.plot_line(ax2, cont2, x, y, ldgs[cont2], '时间：月', '波动率（%）')
    ax2.legend(bbox_to_anchor=(0., 1.02, 1., .202), loc=3,
               ncol=6, mode="expand", borderaxespad=0., frameon=False)
    f2.set_size_inches((12, 6))

    f2.savefig('../save_figure/'+namecode+'_hist_vol_cone_' + str(evalDate) + '.png', dpi=300, format='png')
    df_vol_cone = pd.DataFrame({'1-term': ['6月', '3月', '2月', '1月'],
                                '2-当前隐含波动率': current_vols,
                                '3-最大值': max_vols,
                                '4-最小值': min_vols,
                                '5-中位数': median_vols,
                                '6-75分位数': p75_vols,
                                '7-25分位数': p25_vols})
    df_vol_cone.to_csv('../save_results/'+namecode+'_hist_vol_cone.csv')
    print('Part [历史波动率锥] completed')

"""隐含波动率期限结构"""
def implied_vol_analysis(evalDate,w,nameCode,exchangeCode):

    pu = PlotUtil()
    engine1 = create_engine('mysql+pymysql://readonly:passw0rd@101.132.148.152/mktdata',
                           echo=False)
    Session1 = sessionmaker(bind=engine1)
    sess1 = Session1()

    engine2 = create_engine('mysql+pymysql://readonly:passw0rd@101.132.148.152/metrics',
                           echo=False)
    Session2 = sessionmaker(bind=engine2)
    sess2 = Session2()


    futuresMkt = dbt.FutureMkt
    optionsInfo = dbt.Options
    optionMetrics = dbt.OptionMetrics

    dt_1w = w.tdaysoffset(-1, evalDate, "Period=W").Data[0][0].strftime("%Y-%m-%d")
    dt_2w = w.tdaysoffset(-2, evalDate, "Period=W").Data[0][0].strftime("%Y-%m-%d")
    dt_3w = w.tdaysoffset(-3, evalDate, "Period=W").Data[0][0].strftime("%Y-%m-%d")
    dt_4w = w.tdaysoffset(-4, evalDate, "Period=W").Data[0][0].strftime("%Y-%m-%d")
    dt_5w = w.tdaysoffset(-5, evalDate, "Period=W").Data[0][0].strftime("%Y-%m-%d")
    plt.rcParams['font.sans-serif'] = ['STKaiti']
    plt.rcParams.update({'font.size': 15})

    """波动率期限结构"""
    dates = [evalDate, dt_1w, dt_2w, dt_3w,dt_4w,dt_5w]

    query_f = sess1.query(futureMkt.dt_date, futureMkt.id_instrument.label('id_underlying'),
                            futureMkt.amt_settlement,futureMkt.flag_night,futureMkt.name_code) \
        .filter(or_(futureMkt.dt_date==evalDate,futureMkt.dt_date==dt_1w,futureMkt.dt_date==dt_2w,
                    futureMkt.dt_date==dt_3w,futureMkt.dt_date==dt_4w,futureMkt.dt_date==dt_5w))\
        .filter(futureMkt.name_code == nameCode)\
        .filter(futureMkt.flag_night != 1)

    query_metrics = sess2.query(optionMetrics.dt_date,optionMetrics.id_instrument,optionMetrics.cd_option_type,
                                optionMetrics.pct_implied_vol,optionMetrics.amt_option_price) \
        .filter(or_(optionMetrics.dt_date == evalDate, optionMetrics.dt_date == dt_1w, optionMetrics.dt_date == dt_2w,
                    optionMetrics.dt_date == dt_3w, optionMetrics.dt_date == dt_4w, optionMetrics.dt_date == dt_5w)) \
        # .filter(optionMetrics.name_code == nameCode)

    df_future = pd.read_sql(query_f.statement, query_f.session.bind)
    df_metrics = pd.read_sql(query_metrics.statement, query_metrics.session.bind)
    # df_metrics = df_metrics[(df_metrics['id_instrument'][0] != '5')].reset_index()
    for (idx,row) in df_metrics.iterrows():
        id_option = row['id_instrument']

        if id_option[0] == nameCode or id_option[0:2] == nameCode:
            dt_date = row['dt_date']
            option_price = row['amt_option_price']
            strike = float(id_option[-4:])
            if nameCode == 'm':
                id_underlying = id_option[:6]
            elif nameCode == 'sr':
                id_underlying = id_option[:7]
            else:
                id_underlying = None
            contract_month = id_underlying[-4:]
            if int(contract_month[-2:]) in [1,5,9]:
                df_metrics.loc[idx, 'flag'] = 1
                underlying_price = df_future[(df_future['dt_date']==dt_date)&(df_future['id_underlying']==id_underlying)]['amt_settlement'].values[0]
                df_metrics.loc[idx, 'dt_date'] = dt_date.strftime("%Y-%m-%d")
                df_metrics.loc[idx, 'id_underlying'] = id_underlying
                df_metrics.loc[idx, 'underlying_price'] = underlying_price
                df_metrics.loc[idx, 'contract_month'] = id_underlying[-4:]
                df_metrics.loc[idx, 'diff'] = abs(strike-underlying_price)
            else:
                df_metrics.loc[idx,'flag'] = 0
        else:
            df_metrics.loc[idx,'flag'] = 0

    df_metrics = df_metrics[df_metrics['flag'] == 1].reset_index()
    idx = df_metrics.groupby(['dt_date','id_underlying','cd_option_type'])['diff'].transform(min) == df_metrics['diff']

    # idx = df_metrics.groupby(['dt_date','id_underlying','cd_option_type'])['diff'].max()
    # df_iv = df_metrics[idx].to_frame()
    df_iv = df_metrics[idx]
    df_call_iv = df_iv[df_iv['cd_option_type']=='call'].sort_values(by=['dt_date','id_underlying'],ascending=False).reset_index()# 选取认购平值合约
    df_put_iv = df_iv[df_iv['cd_option_type']=='put'].sort_values(by=['dt_date','id_underlying'],ascending=False).reset_index()# 选取认沽平值合约
    df_call_iv = df_call_iv.drop_duplicates(['dt_date','id_underlying'])
    df_put_iv = df_put_iv.drop_duplicates(['dt_date','id_underlying'])

    optiondata_atm_df = df_put_iv[['dt_date','contract_month']]
    optiondata_atm_df.loc[:,'implied_vol'] = 100*(df_call_iv.loc[:,'pct_implied_vol']+df_put_iv.loc[:,'pct_implied_vol'])/2.0

    f1, ax1 = plt.subplots()
    cont = 0
    contracts = []
    for d in dates:
        df = optiondata_atm_df[optiondata_atm_df['dt_date']==d]
        df = df.sort_values(by=['contract_month'],ascending=True)
        pu.plot_line(ax1, cont, range(len(df)), df['implied_vol'], d, '合约月份', '波动率(%)')
        if len(contracts)==0 :contracts = df['contract_month'].tolist()
        cont += 1
    ax1.legend(bbox_to_anchor=(0., 1.02, 1., .202), loc=3,
               ncol=6, mode="expand", borderaxespad=0.,frameon=False)
    ax1.set_xticks(range(len(contracts)))
    ax1.set_xticklabels(contracts)
    f1.set_size_inches((12,6))
    # optiondata_atm_df = optiondata_atm_df[['date','contract_month','implied_vol']]
    optiondata_atm_df.to_csv('../save_results/'+nameCode+'_implied_vol_term_structure.csv')
    f1.savefig('../save_figure/'+nameCode+'_iv_term_structure_' + str(evalDate) + '.png', dpi=300, format='png')

"""历史隐含波动率"""
def hist_atm_ivs(evalDate,dt_last_week,w,nameCode,exchangeCode,contracts,df_future):
    pu = PlotUtil()
    engine = create_engine('mysql+pymysql://readonly:passw0rd@101.132.148.152/mktdata',
                           echo=False)
    Session = sessionmaker(bind=engine)
    sess = Session()
    engine2 = create_engine('mysql+pymysql://readonly:passw0rd@101.132.148.152/metrics',
                           echo=False)
    Session2 = sessionmaker(bind=engine2)
    sess2 = Session2()

    optionMetrics = dbt.OptionMetrics
    options_table = dbt.Options


    plt.rcParams['font.sans-serif'] = ['STKaiti']
    plt.rcParams.update({'font.size': 15})

    query_sro = sess2.query(optionMetrics.dt_date,optionMetrics.id_instrument,optionMetrics.id_underlying,
                            optionMetrics.amt_strike,
                           optionMetrics.cd_option_type,optionMetrics.pct_implied_vol)\
        .filter(optionMetrics.name_code == nameCode).filter(optionMetrics.dt_date >= dt_last_week)

    query_mdt = sess.query(options_table.id_instrument,options_table.id_underlying,options_table.dt_maturity)\
        .filter(options_table.cd_exchange == exchangeCode)

    df_srf = df_future
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
        for i in range(2):
            iv = df0.loc[i,'pct_implied_vol']
            if iv == 0.0:iv=np.nan
            df_iv_results.loc[idx_dt,'contract-'+str(i+1)] = iv

    df_iv_results = df_iv_results.sort_values(by='dt_date',ascending=False)
    # df = df_iv_results.replace(0.0, None)
    df_iv_results = df_iv_results.dropna()
    core_ivs = df_iv_results['contract-1'].tolist()
    current_iv = core_ivs[0]
    p_75 = np.percentile(core_ivs,75)
    p_25 = np.percentile(core_ivs,25)
    p_mid = np.percentile(core_ivs,50)
    df_iv_results.loc[:,'75分位数（主力合约）'] = p_75
    df_iv_results.loc[:,'25分位数（主力合约）'] = p_25
    df_iv_results.loc[:,'中位数（主力合约）'] = p_mid
    print('hist atm ivs:')
    print('p_75 : ',p_75)
    print('p_25 : ',p_25)
    print('p_mid : ',p_mid)
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
    pu.plot_line(ax1, 1, df_iv_results['dt_date'], [p_75]*len(core_ivs), '75分位数', '日期', '(%)')
    pu.plot_line(ax1, 2, df_iv_results['dt_date'], [p_25]*len(core_ivs), '25分位数', '日期', '(%)')
    pu.plot_line(ax1, 3, df_iv_results['dt_date'], [p_mid]*len(core_ivs), '中位数', '日期', '(%)')

    ax1.legend(bbox_to_anchor=(0., 1.02, 1., .202), loc=3,
               ncol=3, mode="expand", borderaxespad=0.,frameon=False)
    f1.set_size_inches((12,6))
    f1.savefig('../save_figure/'+nameCode+'_hist_atm_ivs_' + str(evalDate) + '.png', dpi=300, format='png')

    df_iv_results.to_csv('../save_results/'+nameCode+'_hist_atm_ivs.csv')

"""当日成交持仓数据"""
def trade_volume(dt_date,dt_last_week,w,nameCode,core_instrumentid):
    w.start()
    pu = PlotUtil()
    engine = create_engine('mysql+pymysql://readonly:passw0rd@101.132.148.152/mktdata',
                           echo=False)
    metadata = MetaData(engine)
    Session = sessionmaker(bind=engine)
    sess = Session()
    options_mkt = Table('options_mktdata', metadata, autoload=True)

    evalDate = dt_date.strftime("%Y-%m-%d")  # Set as Friday
    plt.rcParams['font.sans-serif'] = ['STKaiti']
    plt.rcParams.update({'font.size': 15})

    """当日成交持仓量 """
    query_volume = sess.query(options_mkt.c.dt_date,
                              options_mkt.c.cd_option_type,
                              options_mkt.c.amt_strike,
                              options_mkt.c.amt_holding_volume,
                              options_mkt.c.amt_trading_volume,
                              options_mkt.c.amt_close,
                              options_mkt.c.pct_implied_vol
                              ) \
        .filter(or_(options_mkt.c.dt_date == evalDate,options_mkt.c.dt_date == dt_last_week)) \
        .filter(options_mkt.c.id_underlying == core_instrumentid)\
        .filter(options_mkt.c.flag_night != 1)



    df_2d = pd.read_sql(query_volume.statement, query_volume.session.bind)
    df = df_2d[df_2d['dt_date'] == dt_date].reset_index()
    df_lw = df_2d[df_2d['dt_date'] == dt_last_week].reset_index()
    df_call = df[df['cd_option_type']=='call'].reset_index()
    df_put = df[df['cd_option_type']=='put'].reset_index()
    dflw_call = df_lw[df_lw['cd_option_type']=='call'].reset_index()
    dflw_put = df_lw[df_lw['cd_option_type']=='put'].reset_index()
    call_deltas = []
    put_deltas = []
    for idx,row in df_call.iterrows():
        row_put = df_put.loc[idx]
        strike = row['amt_strike']
        rowlw_call = dflw_call[dflw_call['amt_strike'] == strike]
        rowlw_put = dflw_put[dflw_put['amt_strike'] == strike]
        last_holding_call = 0.0
        last_holding_put = 0.0
        try:
            last_holding_call = rowlw_call['amt_holding_volume'].values[0]
        except:
            pass
        try:
            last_holding_put = rowlw_put['amt_holding_volume'].values[0]
        except:
            pass
        call_delta = row['amt_holding_volume']- last_holding_call
        put_delta = row_put['amt_holding_volume']- last_holding_put
        call_deltas.append(call_delta)
        put_deltas.append(put_delta)
    if nameCode=='sr':
        wt = 25
    else:
        wt = 15
    strikes = df_call['amt_strike'].tolist()
    strikes1 = df_call['amt_strike']+wt
    holding_call = df_call['amt_holding_volume'].tolist()
    holding_put = df_put['amt_holding_volume'].tolist()
    trading_call = df_call['amt_trading_volume'].tolist()
    trading_put = df_put['amt_trading_volume'].tolist()

    df_results = pd.DataFrame({
        '0 call iv':df_call['pct_implied_vol'].tolist(),
        '1 call delta_holding':call_deltas,
        '2 call holding':df_call['amt_holding_volume'].tolist(),
        '3 call trading':df_call['amt_trading_volume'].tolist(),
        '4 call price':df_call['amt_close'].tolist(),
        '5 strikes':df_put['amt_strike'].tolist(),
        '6 put price': df_put['amt_close'].tolist(),
        '7 put trading': df_put['amt_trading_volume'].tolist(),
        '8 put holding': df_put['amt_holding_volume'].tolist(),
        '9 put delta_holding': put_deltas,
        '91 put iv': df_put['pct_implied_vol'].tolist()
    })
    df_results.to_csv('../save_figure/'+nameCode+'_holdings_'+evalDate+'.csv')

    ldgs = ['持仓量（看涨）','持仓量（看跌）','成交量（看涨）','成交量（看跌）']

    f3, ax3 = plt.subplots()
    p1 = ax3.bar(strikes, holding_call,width=wt, color=pu.colors[0])
    p2 = ax3.bar(strikes1, holding_put,width=wt, color=pu.colors[1])
    p3, = ax3.plot(strikes, trading_call, color=pu.colors[2], linestyle=pu.lines[2], linewidth=2)
    p4, = ax3.plot(strikes, trading_put, color=pu.colors[3], linestyle=pu.lines[3], linewidth=2)

    ax3.legend([p1,p2,p3,p4],ldgs,bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
               ncol=4, mode="expand", borderaxespad=0.,frameon=False)
    ax3.spines['top'].set_visible(False)
    ax3.spines['right'].set_visible(False)
    ax3.yaxis.set_ticks_position('left')
    ax3.xaxis.set_ticks_position('bottom')
    f3.set_size_inches((12,6))

    f3.savefig('../save_figure/'+nameCode+'_holdings_' + evalDate + '.png', dpi=300,
                 format='png',bbox_inches='tight')



############################################################################################
# Eval Settings

dt_date = datetime.date(2018, 5, 18)  # Set as Friday
dt_last_week = datetime.date(2018, 5, 11)
current_core_underlying = 'sr_1809'
namecode = 'sr'
exchange_code = 'czce'
# current_core_underlying = 'm_1809'
# namecode = 'm'
# exchange_code = 'dce'
contracts = ['1809', '1901', '1905','1909']

############################################################################################
w.start()
endDate = dt_date
evalDate = dt_date.strftime("%Y-%m-%d")  # Set as Friday
startDate = datetime.date(2017, 1, 1)
hist_date = w.tdaysoffset(-7, startDate, "Period=M").Data[0][0].date()
bd_1m = 21
bd_2m = 2 * bd_1m
bd_3m = 3 * bd_1m
bd_6m = 6 * bd_1m
calendar = ql.China()
pu = PlotUtil()
###########################################################################################
engine2 = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata', echo=False)
metadata2 = MetaData(engine2)
Session2 = sessionmaker(bind=engine2)
sess2 = Session2()
futureMkt = dbt.FutureMkt
optionMkt = dbt.OptionMkt

futuremkt_table = dbt.FutureMkt
options_table = dbt.Options

query_pcr = sess2.query(optionMkt.dt_date, optionMkt.cd_option_type,optionMkt.id_underlying,
                           func.sum(optionMkt.amt_holding_volume).label('total_holding_volume'),
                           func.sum(optionMkt.amt_trading_volume).label('total_trading_volume')
                           ) \
    .filter(optionMkt.dt_date >= startDate) \
    .filter(optionMkt.name_code == namecode) \
    .group_by(optionMkt.cd_option_type, optionMkt.dt_date,optionMkt.id_underlying)

query_srf = sess2.query(futureMkt.dt_date, futureMkt.id_instrument,
                        futureMkt.amt_close, futureMkt.amt_trading_volume,
                        futureMkt.amt_settlement) \
    .filter(futureMkt.dt_date >= hist_date).filter(futureMkt.name_code == namecode)\
    .filter(futureMkt.flag_night != 1)

df_srf = pd.read_sql(query_srf.statement, query_srf.session.bind)
df_pcr = pd.read_sql(query_pcr.statement, query_pcr.session.bind)

df_underlying_core = pcr(df_pcr)
hist_vol(df_underlying_core)

implied_vol_analysis(evalDate,w,namecode,exchange_code)
print('Part [隐含波动率期限结构] completed')
hist_atm_ivs(dt_date,dt_last_week,w,namecode,exchange_code,contracts,df_srf)
print('Part [历史隐含波动率] completed')
trade_volume(dt_date,dt_last_week,w,namecode,current_core_underlying)
print('Part [当日成交持仓量] completed')

plt.show()






















