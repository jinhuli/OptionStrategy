from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol
from back_test.model.base_option_set import BaseOptionSet
from data_access import get_data
import back_test.model.constant as c
import Utilities.admin_util as admin
from sqlalchemy import func
import pandas as pd
import datetime
from pandas import ExcelWriter


"""当日成交持仓数据"""


def trade_volume(dt_date, dt_last_week, df_option_metrics, name_code, core_instrumentid,df_res):
    df = df_option_metrics[
        (df_option_metrics['dt_date'] == dt_date) & (df_option_metrics[c.Util.ID_UNDERLYING] == core_instrumentid)]
    df_lw = df_option_metrics[
        (df_option_metrics['dt_date'] == dt_last_week) & (df_option_metrics[c.Util.ID_UNDERLYING] == core_instrumentid)]
    df_call = df[df['cd_option_type'] == 'call'].reset_index(drop=True)
    df_put = df[df['cd_option_type'] == 'put'].reset_index(drop=True)
    dflw_call = df_lw[df_lw['cd_option_type'] == 'call'].reset_index(drop=True)
    dflw_put = df_lw[df_lw['cd_option_type'] == 'put'].reset_index(drop=True)
    call_deltas = []
    put_deltas = []
    for idx, row in df_call.iterrows():
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
        call_delta = row['amt_holding_volume'] - last_holding_call
        put_delta = row_put['amt_holding_volume'] - last_holding_put
        call_deltas.append(call_delta)
        put_deltas.append(put_delta)

    df_results = pd.DataFrame({
        '0 call iv': df_call['pct_implied_vol'].tolist(),
        '1 call delta_holding': call_deltas,
        '2 call holding': df_call['amt_holding_volume'].tolist(),
        '3 call trading': df_call['amt_trading_volume'].tolist(),
        '4 call price': df_call['amt_close'].tolist(),
        '5 strikes': df_put['amt_strike'].tolist(),
        '6 put price': df_put['amt_close'].tolist(),
        '7 put trading': df_put['amt_trading_volume'].tolist(),
        '8 put holding': df_put['amt_holding_volume'].tolist(),
        '9 put delta_holding': put_deltas,
        '91 put iv': df_put['pct_implied_vol'].tolist()
    })
    return df_results
    #
    # ldgs = ['持仓量（看涨）', '持仓量（看跌）', '成交量（看涨）', '成交量（看跌）']
    #
    # f3, ax3 = plt.subplots()
    # p1 = ax3.bar(strikes, holding_call, width=wt, color=pu.colors[0])
    # p2 = ax3.bar(strikes1, holding_put, width=wt, color=pu.colors[1])
    # p3, = ax3.plot(strikes, trading_call, color=pu.colors[2], linestyle=pu.lines[2], linewidth=2)
    # p4, = ax3.plot(strikes, trading_put, color=pu.colors[3], linestyle=pu.lines[3], linewidth=2)
    #
    # ax3.legend([p1, p2, p3, p4], ldgs, bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
    #            ncol=4, mode="expand", borderaxespad=0., frameon=False)
    # ax3.spines['top'].set_visible(False)
    # ax3.spines['right'].set_visible(False)
    # ax3.yaxis.set_ticks_position('left')
    # ax3.xaxis.set_ticks_position('bottom')
    # f3.set_size_inches((12, 8))
    #
    # f3.savefig('../data/' + name_code + '_holdings.png', dpi=300,
    #            format='png', bbox_inches='tight')
    #
    # f4, ax4 = plt.subplots()
    # p1 = ax4.bar(strikes, call_deltas, width=wt, color=pu.colors[0])
    # p2 = ax4.bar(strikes1, put_deltas, width=wt, color=pu.colors[1])
    # # p3, = ax3.plot(strikes, trading_call, color=pu.colors[2], linestyle=pu.lines[2], linewidth=2)
    # # p4, = ax3.plot(strikes, trading_put, color=pu.colors[3], linestyle=pu.lines[3], linewidth=2)
    #
    # ax4.legend([p1, p2], ['看涨期权持仓量变化', '看跌期权持仓量变化'], borderaxespad=0., frameon=False)
    # ax4.spines['top'].set_visible(False)
    # ax4.spines['right'].set_visible(False)
    # ax4.yaxis.set_ticks_position('left')
    # ax4.xaxis.set_ticks_position('bottom')
    # f4.set_size_inches((12, 8))
    #
    # f4.savefig('../data/' + name_code + '_holding_delta.png', dpi=300,
    #            format='png', bbox_inches='tight')


"""成交持仓认沽认购比P/C"""


def pcr_commodity_option(dt_start, dt_end, name_code, df_res,min_holding):
    optionMkt = admin.table_options_mktdata()
    futureMkt = admin.table_futures_mktdata()
    query_pcr = admin.session_mktdata().query(optionMkt.c.dt_date, optionMkt.c.cd_option_type,
                                              optionMkt.c.id_underlying,
                                              func.sum(optionMkt.c.amt_holding_volume).label('total_holding_volume'),
                                              func.sum(optionMkt.c.amt_trading_volume).label('total_trading_volume')
                                              ) \
        .filter(optionMkt.c.dt_date >= dt_start) \
        .filter(optionMkt.c.dt_date <= dt_end) \
        .filter(optionMkt.c.name_code == name_code) \
        .group_by(optionMkt.c.cd_option_type, optionMkt.c.dt_date, optionMkt.c.id_underlying)
    df_pcr = pd.read_sql(query_pcr.statement, query_pcr.session.bind)
    df_srf = get_data.get_future_c1_by_option_daily(start_date, end_date, name_code, min_holding)
    # 按期权合约持仓量最大选取主力合约
    # df = df_pcr[df_pcr.groupby(['dt_date', 'cd_option_type'])['total_holding_volume'].transform(max) == df_pcr[
    #     'total_holding_volume']]
    # 持仓与成交量计算用所有合约加总
    df = df_pcr.groupby(['dt_date', 'cd_option_type'])['total_holding_volume', 'total_trading_volume'].sum().reset_index()
    df_call = df[df['cd_option_type'] == 'call'].reset_index()
    df_put = df[df['cd_option_type'] == 'put'].reset_index()
    pc_ratio = []
    for idx, row in df_call.iterrows():
        row_put = df_put[df_put['dt_date'] == row['dt_date']]
        pcr_trading = row_put['total_trading_volume'].values[0] / row['total_trading_volume']
        pcr_holding = row_put['total_holding_volume'].values[0] / row['total_holding_volume']
        pc_ratio.append({'dt_date': row['dt_date'],
                         'tv_c': row['total_trading_volume'],
                         'tv_p': row_put['total_trading_volume'].values[0],
                         'hv_c': row['total_holding_volume'],
                         'hv_p': row_put['total_holding_volume'].values[0],
                         'tv_pcr': pcr_trading,
                         'hv_pcr': pcr_holding,
                         })
    df_pcr = pd.DataFrame(pc_ratio)
    df_pcr = pd.merge(df_pcr, df_srf[['dt_date', 'id_instrument', 'amt_close']],
                      how='left', on=['dt_date'], suffixes=['', '_r'])
    df_pcr = df_pcr.sort_values(by='dt_date', ascending=False).reset_index(drop=True)
    df_res.loc[:, name_code + ':A:date'] = df_pcr['dt_date']
    df_res.loc[:, name_code + ':B:tv_c'] = df_pcr['tv_c']
    df_res.loc[:, name_code + ':C:tv_p'] = df_pcr['tv_p']
    df_res.loc[:, name_code + ':E:date'] = df_pcr['dt_date']
    df_res.loc[:, name_code + ':F:hv_c'] = df_pcr['hv_c']
    df_res.loc[:, name_code + ':G:hv_p'] = df_pcr['hv_p']
    df_res.loc[:, name_code + ':I:date'] = df_pcr['dt_date']
    df_res.loc[:, name_code + ':J:tv_pcr'] = df_pcr['tv_pcr']
    df_res.loc[:, name_code + ':K:hv_pcr'] = df_pcr['hv_pcr']
    df_res.loc[:, name_code + ':L:amt_close'] = df_pcr['amt_close']
    return df_res


def pcr_etf_option(dt_start, dt_end, name_code, df_res):
    optionMkt = admin.table_options_mktdata()
    Index_mkt = admin.table_indexes_mktdata()
    query_pcr = admin.session_mktdata().query(optionMkt.c.dt_date, optionMkt.c.cd_option_type,
                                              optionMkt.c.id_underlying,
                                              func.sum(optionMkt.c.amt_holding_volume).label('total_holding_volume'),
                                              func.sum(optionMkt.c.amt_trading_volume).label('total_trading_volume')
                                              ) \
        .filter(optionMkt.c.dt_date >= dt_start) \
        .filter(optionMkt.c.dt_date <= dt_end) \
        .filter(optionMkt.c.name_code == name_code) \
        .group_by(optionMkt.c.cd_option_type, optionMkt.c.dt_date, optionMkt.c.id_underlying)
    df_pcr = pd.read_sql(query_pcr.statement, query_pcr.session.bind)

    query_etf = admin.session_mktdata().query(Index_mkt.c.dt_date, Index_mkt.c.amt_close, Index_mkt.c.amt_open,
                                              Index_mkt.c.id_instrument.label(c.Util.ID_UNDERLYING)) \
        .filter(Index_mkt.c.dt_date >= dt_start).filter(Index_mkt.c.dt_date <= dt_end) \
        .filter(Index_mkt.c.id_instrument == 'index_50etf')
    df_50etf = pd.read_sql(query_etf.statement, query_etf.session.bind)
    df = df_pcr.groupby(['dt_date', 'cd_option_type'])['total_holding_volume', 'total_trading_volume'].sum().reset_index()
    df_call = df[df['cd_option_type'] == 'call'].reset_index()
    df_put = df[df['cd_option_type'] == 'put'].reset_index()
    pc_ratio = []
    for idx, row in df_call.iterrows():
        row_put = df_put[df_put['dt_date'] == row['dt_date']]
        pcr_trading = row_put['total_trading_volume'].values[0] / row['total_trading_volume']
        pcr_holding = row_put['total_holding_volume'].values[0] / row['total_holding_volume']
        pc_ratio.append({'dt_date': row['dt_date'],
                         'tv_c': row['total_trading_volume'],
                         'tv_p': row_put['total_trading_volume'].values[0],
                         'hv_c': row['total_holding_volume'],
                         'hv_p': row_put['total_holding_volume'].values[0],
                         'tv_pcr': pcr_trading,
                         'hv_pcr': pcr_holding,
                         })

    df_pcr = pd.DataFrame(pc_ratio)
    df_pcr = pd.merge(df_pcr, df_50etf[['dt_date', 'amt_close']],
                      how='left', on=['dt_date'], suffixes=['', '_r'])
    df_pcr = df_pcr.sort_values(by='dt_date', ascending=False).reset_index(drop=True)
    df_res.loc[:, name_code + ':A:date'] = df_pcr['dt_date']
    df_res.loc[:, name_code + ':B:tv_c'] = df_pcr['tv_c']
    df_res.loc[:, name_code + ':C:tv_p'] = df_pcr['tv_p']
    df_res.loc[:, name_code + ':E:date'] = df_pcr['dt_date']
    df_res.loc[:, name_code + ':F:hv_c'] = df_pcr['hv_c']
    df_res.loc[:, name_code + ':G:hv_p'] = df_pcr['hv_p']
    df_res.loc[:, name_code + ':I:date'] = df_pcr['dt_date']
    df_res.loc[:, name_code + ':J:tv_pcr'] = df_pcr['tv_pcr']
    df_res.loc[:, name_code + ':K:hv_pcr'] = df_pcr['hv_pcr']
    df_res.loc[:, name_code + ':L:amt_close'] = df_pcr['amt_close']
    return df_res


""" 历史波动率 """


def hist_vol(dt_start, df_future_c1_daily, df_res, name_code):
    m = 100
    df_future_c1_daily.loc[:, 'histvol_10'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE], n=10) * m
    df_future_c1_daily.loc[:, 'histvol_20'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE], n=20) * m
    df_future_c1_daily.loc[:, 'histvol_30'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE], n=30) * m
    df_future_c1_daily.loc[:, 'histvol_60'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE], n=20 * 3) * m
    df_future_c1_daily.loc[:, 'histvol_120'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE], n=20 * 6) * m
    # df_tmp = df_future_c1_daily[df_future_c1_daily[c.Util.DT_DATE] >= dt_start].dropna()
    df_tmp = df_future_c1_daily.sort_values(by=c.Util.DT_DATE, ascending=False).reset_index(drop=True)
    df_res.loc[:, name_code + ':U:date'] = df_tmp[c.Util.DT_DATE]
    df_res.loc[:, name_code + ':V:histvol_10'] = df_tmp.loc[:, 'histvol_10']
    df_res.loc[:, name_code + ':W:histvol_20'] = df_tmp.loc[:, 'histvol_20']
    df_res.loc[:, name_code + ':X:histvol_30'] = df_tmp.loc[:, 'histvol_30']
    df_res.loc[:, name_code + ':Y:histvol_60'] = df_tmp.loc[:, 'histvol_60']
    df_res.loc[:, name_code + ':Z:histvol_120'] = df_tmp.loc[:, 'histvol_120']
    return df_res


""" 隐含波动率 """


def implied_vol_avg(last_week, end_date, df_metrics, df_res, name_code):
    m = 100
    optionset = BaseOptionSet(df_metrics, rf=0.03)
    optionset.init()
    list_res_iv = []
    while optionset.current_index < optionset.nbr_index:
        dt_maturity = optionset.select_maturity_date(nbr_maturity=0, min_holding=min_holding)
        list_atm_call, list_atm_put = optionset.get_options_list_by_moneyness_mthd1(moneyness_rank=0,
                                                                                    maturity=dt_maturity)
        atm_call = optionset.select_higher_volume(list_atm_call)
        atm_put = optionset.select_higher_volume(list_atm_put)
        iv_call = atm_call.get_implied_vol()
        iv_put = atm_put.get_implied_vol()
        iv = (iv_put + iv_call) / 2.0
        list_res_iv.append({'date': optionset.eval_date, 'iv': iv})
        if not optionset.has_next(): break
        optionset.next()
    df_iv = pd.DataFrame(list_res_iv).sort_values(by='date', ascending=False).reset_index(drop=True)
    df_res.loc[:, name_code + ':N:date'] = df_iv['date']
    df_res.loc[:, name_code + ':O:iv'] = df_iv['iv'] * m
    return df_res


def implied_vol(last_week, end_date, df_metrics, df_res, name_code):
    m = 100
    optionset = BaseOptionSet(df_metrics, rf=0.03)
    optionset.init()
    list_res_iv = []
    while optionset.current_index < optionset.nbr_index:
        dt_maturity = optionset.select_maturity_date(nbr_maturity=0, min_holding=min_holding)
        iv = optionset.get_atm_iv_by_htbr(dt_maturity)
        list_res_iv.append({'date': optionset.eval_date, 'iv': iv})
        if not optionset.has_next(): break
        optionset.next()
    df_iv = pd.DataFrame(list_res_iv).sort_values(by='date', ascending=False).reset_index(drop=True)
    df_res.loc[:, name_code + ':N:date'] = df_iv['date']
    df_res.loc[:, name_code + ':O:iv'] = df_iv['iv'] * m
    return df_res


def implied_vol_vw(last_week, end_date, df_metrics, df_res, name_code):
    m = 100
    optionset = BaseOptionSet(df_metrics, rf=0.0)
    optionset.init()
    list_res_iv = []
    while optionset.current_index < optionset.nbr_index:
        dt_maturity = optionset.select_maturity_date(nbr_maturity=0, min_holding=min_holding)
        iv_volume_weighted = optionset.get_volume_weighted_iv(dt_maturity)
        list_res_iv.append({'date': optionset.eval_date, 'iv': iv_volume_weighted})
        if not optionset.has_next(): break
        optionset.next()
    df_iv = pd.DataFrame(list_res_iv).sort_values(by='date', ascending=False).reset_index(drop=True)
    df_res.loc[:, name_code + ':N:date'] = df_iv['date']
    df_res.loc[:, name_code + ':O:iv'] = df_iv['iv'] * m
    return df_res


""""""

end_date = datetime.date.today()
start_date = datetime.date(2017, 1, 1)
last_week = datetime.date(2018,10,26)
dt_histvol = datetime.date(2015,1,1)
min_holding = 5


writer = ExcelWriter('../data/option_data_python.xlsx')
name_codes = [c.Util.STR_50ETF,c.Util.STR_CU, c.Util.STR_M, c.Util.STR_SR]
core_ids = ['index_50etf','cu_1901', 'm_1901', 'sr_1901']
for (idx, name_code) in enumerate(name_codes):
    df_res = pd.DataFrame()
    core_id = core_ids[idx]
    if name_code==c.Util.STR_50ETF:
        df_metrics = get_data.get_50option_mktdata(start_date,end_date)
        df_future_c1_daily = get_data.get_mktdata_future_c1_daily(dt_histvol, end_date, c.Util.STR_IH)
    else:
        df_metrics = get_data.get_comoption_mktdata(start_date, end_date, name_code)
        df_future_c1_daily = get_data.get_future_c1_by_option_daily(dt_histvol, end_date, name_code, min_holding)
    df_res = hist_vol(start_date, df_future_c1_daily, df_res, name_code)
    dt_start = max(df_metrics[c.Util.DT_DATE].values[0], df_future_c1_daily[c.Util.DT_DATE].values[0])
    df_metrics = df_metrics[(df_metrics[c.Util.DT_DATE] >= dt_start) & (df_metrics[c.Util.DT_DATE] <= end_date)].reset_index(
            drop=True)
    if name_code == c.Util.STR_50ETF:
        df_res = pcr_etf_option(dt_start, end_date, name_code, df_res)
    else:
        df_res = pcr_commodity_option(dt_start, end_date, name_code, df_res,min_holding)
    df_res = implied_vol(dt_start, end_date, df_metrics, df_res, name_code)

    df_res.to_excel(writer, name_code)
    dt_end = df_metrics[c.Util.DT_DATE].unique()[-1]
    dt_yesterday = df_metrics[c.Util.DT_DATE].unique()[-2]
    print(dt_end,dt_yesterday)
    df_holdings = trade_volume(dt_end, dt_yesterday, df_metrics, name_code, core_id,df_res)
    df_holdings.to_excel(writer, 'holdings_'+name_code)
writer.save()
