from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from mpl_toolkits.mplot3d import Axes3D
import matplotlib as mpl
from mpl_toolkits.axes_grid1 import host_subplot
from matplotlib.dates import date2num
from matplotlib import cm as plt_cm
import datetime
import pandas as pd
import numpy as np
from WindPy import w
from data_access.db_tables import DataBaseTables as dbt
import matplotlib.pyplot as plt
from Utilities.PlotUtil import PlotUtil
import QuantLib as ql


"""隐含波动率期限结构"""
def implied_vol_analysis(evalDate,w,nameCode,exchangeCode):

    pu = PlotUtil()
    engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata',
                           echo=False)
    Session = sessionmaker(bind=engine)
    sess = Session()
    futuremkt_table = dbt.FutureMkt
    optionmkt_table = dbt.OptionMkt
    options_table = dbt.Options

    # hist_date = w.tdaysoffset(-2, evalDate, "Period=Y").Data[0][0].strftime("%Y-%m-%d")
    evalDate_1week = w.tdaysoffset(-1, evalDate, "Period=W").Data[0][0].strftime("%Y-%m-%d")
    evalDate_2week = w.tdaysoffset(-2, evalDate, "Period=W").Data[0][0].strftime("%Y-%m-%d")
    evalDate_3week = w.tdaysoffset(-3, evalDate, "Period=W").Data[0][0].strftime("%Y-%m-%d")
    evalDate_4week = w.tdaysoffset(-4, evalDate, "Period=W").Data[0][0].strftime("%Y-%m-%d")
    evalDate_5week = w.tdaysoffset(-5, evalDate, "Period=W").Data[0][0].strftime("%Y-%m-%d")
    plt.rcParams['font.sans-serif'] = ['STKaiti']
    plt.rcParams.update({'font.size': 15})


    """波动率期限结构"""
    dates = [evalDate, evalDate_1week, evalDate_2week, evalDate_3week,evalDate_4week,evalDate_5week]
    optiondata_df = pd.DataFrame()
    columns = [
        'date', 'id_instrument', 'implied_vol', 'contract_month',
        'option_type', 'strike', 'underlying_price', 'atm_dif']
    optiondata_atm_df = pd.DataFrame(columns=columns)
    idx_o = 0
    for date in dates:
        optiondataset = sess.query(optionmkt_table, options_table, futuremkt_table) \
            .join(futuremkt_table, optionmkt_table.id_underlying == futuremkt_table.id_instrument) \
            .join(options_table, optionmkt_table.id_instrument == options_table.id_instrument) \
            .filter(optionmkt_table.dt_date == date) \
            .filter(optionmkt_table.datasource == exchangeCode) \
            .filter(optionmkt_table.flag_night != 1) \
            .filter(futuremkt_table.dt_date == date) \
            .filter(futuremkt_table.name_code == nameCode) \
            .filter(futuremkt_table.flag_night != 1) \
            .all()

        contract_months = []
        for optiondata in optiondataset:
            if optiondata.Options.cd_option_type == 'put': continue
            if int(optiondata.Options.name_contract_month[-2:]) not in [1,5,9]: continue
            optiondata_df.loc[idx_o, 'date'] = date
            optiondata_df.loc[idx_o, 'id_instrument'] = optiondata.OptionMkt.id_instrument
            optiondata_df.loc[idx_o, 'implied_vol'] = optiondata.OptionMkt.pct_implied_vol
            optiondata_df.loc[idx_o, 'contract_month'] = optiondata.Options.name_contract_month
            optiondata_df.loc[idx_o, 'option_type'] = optiondata.Options.cd_option_type
            optiondata_df.loc[idx_o, 'strike'] = optiondata.Options.amt_strike
            optiondata_df.loc[idx_o, 'underlying_price'] = optiondata.FutureMkt.amt_settlement
            optiondata_df.loc[idx_o, 'atm_dif'] = abs(
                optiondata.Options.amt_strike - optiondata.FutureMkt.amt_settlement)
            cm = optiondata.Options.name_contract_month
            if optiondata.Options.name_contract_month not in contract_months:
                contract_months.append(optiondata.Options.name_contract_month)
            idx_o += 1

        for cm1 in contract_months:
            c = optiondata_df['contract_month'].map(lambda x: x == cm1)
            c1 = optiondata_df['date'].map(lambda x: x == date)
            critiron = c & c1
            df = optiondata_df[critiron]
            idx = df['atm_dif'].idxmin()
            optiondata_atm_df = optiondata_atm_df.append(df.loc[idx], ignore_index=True)
    print('atm_implied_vols')
    print(optiondata_atm_df)
    f1, ax1 = plt.subplots()
    cont = 0
    contracts = []
    for d in dates:
        c2 = optiondata_atm_df['date'].map(lambda a: a == d)
        df = optiondata_atm_df[c2]
        pu.plot_line(ax1, cont, range(len(df)), df['implied_vol'], d, '合约月份', '波动率(%)')
        if len(contracts)==0 :contracts = df['contract_month'].tolist()
        cont += 1
    ax1.legend(bbox_to_anchor=(0., 1.02, 1., .202), loc=3,
               ncol=6, mode="expand", borderaxespad=0.,frameon=False)
    ax1.set_xticks(range(len(contracts)))
    ax1.set_xticklabels(contracts)
    f1.set_size_inches((12,6))
    optiondata_atm_df = optiondata_atm_df[['date','contract_month','implied_vol']]
    optiondata_atm_df.to_csv('../save_results/'+nameCode+'_implied_vol_term_structure.csv')
    f1.savefig('../save_figure/'+nameCode+'_atm_iv_term_structure_' + str(evalDate) + '.png', dpi=300, format='png')
    #
    # #################### Futures and Realised Vol
    # # Get core contract mktdata
    # data = w.wsd("M.DCE", "trade_hiscode", hist_date, evalDate, "")
    # underlying_df = pd.DataFrame({'date': data.Times, 'code_core': data.Data[0]})
    #
    # futuredataset = sess.query(futuremkt_table) \
    #     .filter(futuremkt_table.dt_date >= hist_date) \
    #     .filter(futuremkt_table.dt_date <= evalDate) \
    #     .filter(futuremkt_table.name_code == nameCode).all()
    #
    # id_instruments = []
    # future_closes = []
    #
    # for idx in underlying_df.index:
    #     row = underlying_df.loc[idx]
    #     dt_date = row['date']
    #     code_instrument = row['code_core']
    #     id_instrument = 'm_1' + code_instrument[2:5]
    #     id_instruments.append(id_instrument)
    #     amt_close = 0.0
    #     for future in futuredataset:
    #         if future.dt_date == dt_date and future.id_instrument == id_instrument:
    #             amt_close = future.amt_settlement
    #     future_closes.append(amt_close)
    # underlying_df['id_core'] = id_instruments
    # underlying_df['price'] = future_closes
    #
    # future_yields = []
    # for idx_c, price in enumerate(future_closes):
    #     if idx_c == 0:
    #         r = 0.0
    #     else:
    #         price = float(price)
    #         future_close = float(future_closes[idx_c - 1])
    #         if price == 0.0 or future_close == 0.0: r = 0.0
    #         else : r = np.log(price/future_close)
    #     future_yields.append(r)
    # underlying_df['yield'] = future_yields
    # underlying_df = underlying_df[underlying_df['yield'] != 0.0].reset_index()
    # # print(underlying_df)
    #
    # histvols_6 = []
    # histvols_3 = []
    # histvols_2 = []
    # histvols_1 = []
    # for idx_v in range(121, len(underlying_df['price'])):
    #     histvols_6.append(np.std(underlying_df['yield'][idx_v - 120:idx_v]) * np.sqrt(252) * 100)
    #     histvols_3.append(np.std(underlying_df['yield'][idx_v - 60:idx_v]) * np.sqrt(252) * 100)
    #     histvols_2.append(np.std(underlying_df['yield'][idx_v - 40:idx_v]) * np.sqrt(252) * 100)
    #     histvols_1.append(np.std(underlying_df['yield'][idx_v - 20:idx_v]) * np.sqrt(252) * 100)
    # underlying_df.loc[121:, 'histvol_6'] = histvols_6
    # underlying_df.loc[121:, 'histvol_3'] = histvols_3
    # underlying_df.loc[121:, 'histvol_2'] = histvols_2
    # underlying_df.loc[121:, 'histvol_1'] = histvols_1
    # max_vols = [max(histvols_6), max(histvols_3), max(histvols_2), max(histvols_1)]
    # min_vols = [min(histvols_6), min(histvols_3), min(histvols_2), min(histvols_1)]
    # median_vols = [np.median(histvols_6), np.median(histvols_3), np.median(histvols_2),
    #                np.median(histvols_1)]
    # p75_vols = [np.percentile(histvols_6, 75), np.percentile(histvols_3, 75),
    #             np.percentile(histvols_2, 75), np.percentile(histvols_1, 75)]
    # p25_vols = [np.percentile(histvols_6, 25), np.percentile(histvols_3, 25),
    #             np.percentile(histvols_2, 25), np.percentile(histvols_1, 25)]
    # current_vols = [histvols_6[-1], histvols_3[-1], histvols_2[-1], histvols_1[-1]]
    # print('current_vols : ', current_vols)
    # histvolcone = [current_vols, max_vols, min_vols, median_vols, p75_vols, p25_vols]
    # x = [6, 3, 2, 1]
    # f2, ax2 = plt.subplots()
    # ldgs = ['当前水平', '最大值', '最小值', '中位数', '75分位数', '25分位数']
    # for cont2, y in enumerate(histvolcone):
    #     pu.plot_line(ax2, cont2, x, y, ldgs[cont2], '时间：月', '波动率（%）')
    # ax2.legend(bbox_to_anchor=(0., 1.02, 1., .202), loc=3,
    #            ncol=6, mode="expand", borderaxespad=0.,frameon=False)
    # f2.set_size_inches((12,6))
    #
    # f2.savefig('../save_figure/sr_hist_vol_cone_' + str(evalDate) + '.png', dpi=300, format='png')
    # df_vol_cone = pd.DataFrame({'1-term':['6月','3月','2月','1月'],
    #                             '2-当前隐含波动率':current_vols,
    #                             '3-最大值':max_vols,
    #                             '4-最小值':min_vols,
    #                             '5-中位数':median_vols,
    #                             '6-75分位数':p75_vols,
    #                             '7-25分位数':p25_vols})
    # df_vol_cone.to_csv('../save_results/sr_hist_vol_cone.csv')

    # ################ #Implied Vol Surface
    # dates_week = [evalDate,w.tdaysoffset(-5, evalDate, "").Data[0][0].strftime("%Y-%m-%d")]
    # black_var_surfaces = []
    # for dt in dates_week:
    #     optionivs_df = pd.DataFrame()
    #     date = evalDate
    #     dt_eval = datetime.datetime.strptime(evalDate, '%Y-%m-%d').date()
    #     ql_evalDate = ql.Date(dt_eval.day, dt_eval.month, dt_eval.year)
    #     calendar = ql.China()
    #     daycounter = ql.ActualActual()
    #     optiondataset = sess.query(optionmkt_table, options_table) \
    #         .join(options_table, optionmkt_table.id_instrument == options_table.id_instrument) \
    #         .filter(optionmkt_table.dt_date == date) \
    #         .filter(optionmkt_table.datasource == 'czce') \
    #         .all()
    #     idx_ivs = 0
    #     for optiondata in optiondataset:
    #         if optiondata.Options.cd_option_type == 'put': continue
    #         if optiondata.Options.name_contract_month not in contracts: continue
    #         optionivs_df.loc[idx_ivs, 'id_instrument'] = optiondata.OptionMkt.id_instrument
    #         optionivs_df.loc[idx_ivs, 'pct_implied_vol'] = float(optiondata.OptionMkt.pct_implied_vol)
    #         optionivs_df.loc[idx_ivs, 'dt_maturity'] = optiondata.Options.dt_maturity
    #         optionivs_df.loc[idx_ivs, 'amt_strike'] = float(optiondata.Options.amt_strike)
    #         idx_ivs += 1
    #
    #     maturities = optionivs_df['dt_maturity'].unique()
    #     strikes = optionivs_df['amt_strike'].unique()
    #     volset = []
    #     year_fracs = []
    #     core_strikes = []
    #     ql_maturities = []
    #     for k in strikes:
    #         nbr_k = len(optionivs_df[optionivs_df['amt_strike'].map(lambda x: x == k)])
    #         if nbr_k == maturities.size:
    #             core_strikes.append(k)
    #     for dt_m in maturities:
    #         c0 = optionivs_df['dt_maturity'].map(lambda x: x == dt_m)
    #         volset.append(optionivs_df[c0]['pct_implied_vol'].values.tolist())
    #         year_fracs.append((dt_m - dt_eval).days / 365.0)
    #         ql_maturities.append(ql.Date(dt_m.day, dt_m.month, dt_m.year))
    #
    #     implied_vols = ql.Matrix(len(core_strikes), len(maturities))
    #     for i in range(implied_vols.rows()):
    #         for j in range(implied_vols.columns()):
    #             implied_vols[i][j] = volset[j][i]
    #     plot_years = np.arange(min(year_fracs), max(year_fracs), 0.01)
    #     plot_strikes = np.arange(min(core_strikes), max(core_strikes), 10.0)
    #     black_var_surface = ql.BlackVarianceSurface(
    #         ql_evalDate, calendar, ql_maturities, core_strikes, implied_vols,
    #         daycounter)
    #     black_var_surfaces.append(black_var_surface)
    #
    # X, Y = np.meshgrid(plot_strikes, plot_years)
    # Z = np.array([black_var_surfaces[0].blackVol(y, x)
    #               for xr, yr in zip(X, Y)
    #               for x, y in zip(xr, yr)]
    #              ).reshape(len(X), len(X[0]))
    #
    # fig2 = plt.figure()
    # ax_ivs2 = fig2.gca(projection='3d')
    # surf2 = ax_ivs2.plot_surface(X, Y, Z, rstride=1, cstride=1, cmap=plt_cm.coolwarm, linewidth=0.1)
    # ax_ivs2.set_xlabel('行权价')
    # ax_ivs2.set_ylabel('期限')
    # ax_ivs2.set_zlabel('波动率（%）')
    # fig2.colorbar(surf2, shrink=0.5, aspect=5)
    #
    # fig2.savefig('../save_figure/sr_iv_surface_' + str(evalDate) + '.png', dpi=300, format='png')

"""历史隐含波动率"""
def hist_atm_ivs(evalDate,w,nameCode,exchangeCode,contracts,df_future):
    pu = PlotUtil()
    engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata',
                           echo=False)
    Session = sessionmaker(bind=engine)
    sess = Session()
    # futuremkt_table = dbt.FutureMkt
    optionmkt_table = dbt.OptionMkt
    options_table = dbt.Options

    # startDate = datetime.date(2017, 4, 19).strftime("%Y-%m-%d")

    plt.rcParams['font.sans-serif'] = ['STKaiti']
    plt.rcParams.update({'font.size': 15})

    query_sro = sess.query(optionmkt_table.dt_date,optionmkt_table.id_instrument,optionmkt_table.id_underlying,
                           optionmkt_table.amt_strike,optionmkt_table.cd_option_type,optionmkt_table.pct_implied_vol)\
        .filter(optionmkt_table.name_code == nameCode)\
        .filter(optionmkt_table.datasource == exchangeCode) \
        .filter(optionmkt_table.flag_night != 1)

    query_mdt = sess.query(options_table.id_instrument,options_table.id_underlying,options_table.dt_maturity)\
        .filter(options_table.cd_exchange == exchangeCode)

    # query_srf = sess.query(futuremkt_table.dt_date, futuremkt_table.id_instrument,
    #                        futuremkt_table.amt_close, futuremkt_table.amt_trading_volume,futuremkt_table.amt_settlement) \
    #     .filter(futuremkt_table.name_code == 'sr')

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
            # try:
            amt_settle = df_srf[(df_srf['dt_date']==date)&(df_srf['id_instrument']==underlying)]['amt_settlement'].values[0]
            # except:
            #     print(date,' , ',id_instrument)
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
            df_iv_results.loc[idx_dt,'contract-'+str(i+1)] = df0.loc[i,'pct_implied_vol']

    df_iv_results = df_iv_results.sort_values(by='dt_date',ascending=False)

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
    engine = create_engine('mysql+pymysql://guest:passw0rd@101.132.148.152/mktdata',
                           echo=False)
    metadata = MetaData(engine)
    Session = sessionmaker(bind=engine)
    sess = Session()
    options_mkt = Table('options_mktdata', metadata, autoload=True)
    # futures_mkt = Table('futures_mktdata', metadata, autoload=True)

    evalDate = dt_date.strftime("%Y-%m-%d")  # Set as Friday
    # start_date = w.tdaysoffset(-3, evalDate, "Period=M").Data[0][0].strftime("%Y-%m-%d")
    plt.rcParams['font.sans-serif'] = ['STKaiti']
    plt.rcParams.update({'font.size': 15})
    # nameCode = 'sr'
    # core_instrumentid = 'sr_1805'

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
    wt = 30
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

    ################################# 成交持仓认沽认购比P/C #################################################
    # query_volume = sess.query(options_mkt.c.dt_date, options_mkt.c.cd_option_type,
    #                           func.sum(options_mkt.c.amt_holding_volume).label('total_holding_volume'),
    #                           func.sum(options_mkt.c.amt_trading_volume).label('total_trading_volume')
    #                           ) \
    #     .filter(options_mkt.c.dt_date <= evalDate) \
    #     .filter(options_mkt.c.dt_date >= start_date) \
    #     .filter(options_mkt.c.name_code == nameCode) \
    #     .group_by(options_mkt.c.cd_option_type, options_mkt.c.dt_date)
    #
    # query_future = sess.query(futures_mkt.c.dt_date, futures_mkt.c.amt_close.label('future_close')) \
    #     .filter(futures_mkt.c.dt_date <= evalDate) \
    #     .filter(futures_mkt.c.dt_date >= start_date) \
    #     .filter(futures_mkt.c.id_instrument == core_instrumentid)
    #
    # df = pd.read_sql(query_volume.statement, query_volume.session.bind)
    # df_future = pd.read_sql(query_future.statement, query_future.session.bind)
    #
    # df_call = df[df['cd_option_type'] == 'call'].reset_index()
    # df_put = df[df['cd_option_type'] == 'put'].reset_index()
    # pc_ratio = []
    # for idx, row in df_call.iterrows():
    #     row_put = df_put[df_put['dt_date'] == row['dt_date']]
    #     pcr_trading = row_put['total_trading_volume'].values[0] / row['total_trading_volume']
    #     pcr_holding = row_put['total_holding_volume'].values[0] / row['total_holding_volume']
    #     pc_ratio.append({'dt_date': row['dt_date'], 'pcr_trading': pcr_trading, 'pcr_holding': pcr_holding})
    #
    # df_pcr = pd.DataFrame(pc_ratio)
    #
    # df_pcr = df_pcr.join(df_future.set_index('dt_date'), on='dt_date')
    # print(df_pcr[df_pcr['dt_date'] == dt_date])
    # print(df_pcr)
    # fig1 = plt.figure()
    # host = host_subplot(111)
    # par = host.twinx()
    # ldgs = [ '持仓量P/C', '成交量P/C','期货价格（左）']
    # x = df_pcr['dt_date'].tolist()
    # p1, = par.plot(x, df_pcr['pcr_holding'].tolist(),
    #         color = pu.colors[0], linestyle = pu.lines[0], linewidth = 2)
    # p2, = par.plot(x, df_pcr['pcr_trading'].tolist(),
    #         color=pu.colors[1], linestyle=pu.lines[1], linewidth=2)
    # p3, = host.plot(x, df_pcr['future_close'].tolist(),
    #         color=pu.colors[2], linestyle=pu.lines[2], linewidth=2)
    # host.legend([p1,p2,p3],ldgs,bbox_to_anchor=(0., 1.02, 1., .202), loc=3,
    #             ncol=3, mode="expand", borderaxespad=0.,frameon=False)
    # host.spines['top'].set_visible(False)
    # host.yaxis.set_ticks_position('left')
    # host.xaxis.set_ticks_position('bottom')
    # for label in host.get_xmajorticklabels():
    #     label.set_rotation(90)
    #     label.set_horizontalalignment("right")
    # fig1.set_size_inches((12,6))
    #
    # fig1.savefig('../save_figure/sr_holdings_pcr_' + evalDate + '.png', dpi=300,
    #              format='png',bbox_inches='tight')

###################################################################################
# w.start()
# dt_date = datetime.date(2018, 2, 23)  # Set as Friday
# dt_last_week = datetime.date(2018, 2, 9)
#
# evalDate = dt_date.strftime("%Y-%m-%d")
# sr_implied_vol_analysis(evalDate,w)
# sr_hist_atm_ivs(evalDate,w)
# sr_pcr_analysis(dt_date,dt_last_week,w)
# plt.show()
