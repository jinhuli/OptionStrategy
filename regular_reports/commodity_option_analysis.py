from back_test.model.base_option_set import BaseOptionSet
from data_access import get_data
import back_test.model.constant as c
import datetime
from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
from Utilities.timebase import LLKSR
import Utilities.admin_util as admin
import pandas as pd
from sqlalchemy import func

"""当日成交持仓数据"""


def trade_volume(dt_date, dt_last_week, df_option_metrics, name_code,core_instrumentid):
    pu = PlotUtil()
    df = df_option_metrics[(df_option_metrics['dt_date'] == dt_date)&(df_option_metrics[c.Util.ID_UNDERLYING] == core_instrumentid)]
    df_lw = df_option_metrics[(df_option_metrics['dt_date'] == dt_last_week)&(df_option_metrics[c.Util.ID_UNDERLYING] == core_instrumentid)]
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
    if name_code == c.Util.STR_SR:
        wt = 25
    else:
        wt = 15
    strikes = df_call['amt_strike'].tolist()
    strikes1 = df_call['amt_strike'] + wt
    holding_call = df_call['amt_holding_volume'].tolist()
    holding_put = df_put['amt_holding_volume'].tolist()
    trading_call = df_call['amt_trading_volume'].tolist()
    trading_put = df_put['amt_trading_volume'].tolist()

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
    df_results.to_csv('../data/' + name_code + '_holdings.csv')

    ldgs = ['持仓量（看涨）', '持仓量（看跌）', '成交量（看涨）', '成交量（看跌）']

    f3, ax3 = plt.subplots()
    p1 = ax3.bar(strikes, holding_call, width=wt, color=pu.colors[0])
    p2 = ax3.bar(strikes1, holding_put, width=wt, color=pu.colors[1])
    p3, = ax3.plot(strikes, trading_call, color=pu.colors[2], linestyle=pu.lines[2], linewidth=2)
    p4, = ax3.plot(strikes, trading_put, color=pu.colors[3], linestyle=pu.lines[3], linewidth=2)

    ax3.legend([p1, p2, p3, p4], ldgs, bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
               ncol=4, mode="expand", borderaxespad=0., frameon=False)
    ax3.spines['top'].set_visible(False)
    ax3.spines['right'].set_visible(False)
    ax3.yaxis.set_ticks_position('left')
    ax3.xaxis.set_ticks_position('bottom')
    f3.set_size_inches((12, 8))

    f3.savefig('../data/' + name_code + '_holdings.png', dpi=300,
               format='png', bbox_inches='tight')


"""成交持仓认沽认购比P/C"""


def pcr(dt_start, name_code, df_res):
    optionMkt = admin.table_options_mktdata()
    futureMkt = admin.table_futures_mktdata()
    query_pcr = admin.session_mktdata().query(optionMkt.c.dt_date, optionMkt.c.cd_option_type,
                                              optionMkt.c.id_underlying,
                                              func.sum(optionMkt.c.amt_holding_volume).label('total_holding_volume'),
                                              func.sum(optionMkt.c.amt_trading_volume).label('total_trading_volume')
                                              ) \
        .filter(optionMkt.c.dt_date >= dt_start) \
        .filter(optionMkt.c.name_code == name_code) \
        .group_by(optionMkt.c.cd_option_type, optionMkt.c.dt_date, optionMkt.c.id_underlying)
    query_srf = admin.session_mktdata().query(futureMkt.c.dt_date, futureMkt.c.id_instrument,
                                              futureMkt.c.amt_close, futureMkt.c.amt_trading_volume,
                                              futureMkt.c.amt_settlement) \
        .filter(futureMkt.c.dt_date >= dt_start).filter(futureMkt.c.name_code == name_code) \
        .filter(futureMkt.c.flag_night != 1)
    df_pcr = pd.read_sql(query_pcr.statement, query_pcr.session.bind)
    df_srf = pd.read_sql(query_srf.statement, query_srf.session.bind)
    # 按期权合约持仓量最大选取主力合约
    df = df_pcr[df_pcr.groupby(['dt_date', 'cd_option_type'])['total_holding_volume'].transform(max) == df_pcr[
        'total_holding_volume']]
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
                         'id_instrument': row['id_underlying'],
                         })

    df_pcr = pd.DataFrame(pc_ratio)
    df_pcr = pd.merge(df_pcr, df_srf[['dt_date', 'id_instrument', 'amt_settlement']], how='left',
                      on=['dt_date', 'id_instrument'],
                      suffixes=['', '_r'])
    df_pcr = df_pcr.sort_values(by='dt_date', ascending=False)
    df_res.loc[:, 'A:date'] = df_pcr['dt_date']
    df_res.loc[:, 'B:tv_c'] = df_pcr['tv_c']
    df_res.loc[:, 'C:tv_p'] = df_pcr['tv_p']
    df_res.loc[:, 'D'] = None
    df_res.loc[:, 'E:date'] = df_pcr['dt_date']
    df_res.loc[:, 'F:hv_c'] = df_pcr['hv_c']
    df_res.loc[:, 'G:hv_p'] = df_pcr['hv_p']
    df_res.loc[:, 'H'] = None
    df_res.loc[:, 'I:date'] = df_pcr['dt_date']
    df_res.loc[:, 'J:tv_pcr'] = df_pcr['tv_pcr']
    df_res.loc[:, 'K:hv_pcr'] = df_pcr['hv_pcr']
    df_res.loc[:, 'L:amt_settlement'] = df_pcr['amt_settlement']
    df_res.loc[:, 'M'] = None
    return df_res


""" 历史波动率 """


def hist_vol(dt_start, df_future_c1_daily, df_res):
    df_future_c1_daily.loc[:, 'histvol_10'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE], n=10)
    df_future_c1_daily.loc[:, 'histvol_20'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE], n=20)
    df_future_c1_daily.loc[:, 'histvol_60'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE], n=20 * 3)
    df_future_c1_daily.loc[:, 'histvol_120'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE], n=20 * 6)
    df_tmp = df_future_c1_daily[df_future_c1_daily[c.Util.DT_DATE] >= dt_start].dropna().reset_index(drop=True)
    df_res.loc[:, 'N:date'] = df_tmp[c.Util.DT_DATE]
    df_res.loc[:, 'O:histvol_10'] = df_tmp.loc[:, 'histvol_10']
    df_res.loc[:, 'P:histvol_20'] = df_tmp.loc[:, 'histvol_20']
    df_res.loc[:, 'Q:histvol_60'] = df_tmp.loc[:, 'histvol_60']
    df_res.loc[:, 'R:histvol_120'] = df_tmp.loc[:, 'histvol_120']
    df_res.loc[:, 'S'] = None
    return df_res


def implied_vol(dt_start, df_iv, df_res):
    df_tmp = df_iv[df_iv[c.Util.DT_DATE] >= dt_start].reset_index(drop=True)
    df_res.loc[:, 'T:date'] = df_tmp[c.Util.DT_DATE]
    df_res.loc[:, 'U:iv'] = df_tmp['average_iv'] *100
    return df_res


def implied_vol_term_structure(dt_list):
    iv_term_structure = []
    df_iv_atm_1 = get_data.get_iv_by_moneyness(dt_list[0], dt_list[-1], name_code, nbr_moneyness=0,
                                               cd_mdt_selection='hp_8_1st')
    df_iv_atm_2 = get_data.get_iv_by_moneyness(dt_list[0], dt_list[-1], name_code, nbr_moneyness=0,
                                               cd_mdt_selection='hp_8_2nd')
    df_iv_atm_3 = get_data.get_iv_by_moneyness(dt_list[0], dt_list[-1], name_code, nbr_moneyness=0,
                                               cd_mdt_selection='hp_8_3rd')
    for dt_date in dt_list:
        iv_call_1 = df_iv_atm_1[
            (df_iv_atm_1[c.Util.DT_DATE] == dt_date) & (df_iv_atm_1[c.Util.CD_OPTION_TYPE] == 'call')][c.Util.PCT_IMPLIED_VOL].values[0]
        iv_put_1 = df_iv_atm_1[(df_iv_atm_1[c.Util.DT_DATE] == dt_date) & (df_iv_atm_1[c.Util.CD_OPTION_TYPE] == 'put')][c.Util.PCT_IMPLIED_VOL].values[0]
        iv_1 = (iv_call_1 + iv_put_1) / 2
        iv_call_2 = df_iv_atm_2[
            (df_iv_atm_2[c.Util.DT_DATE] == dt_date) & (df_iv_atm_2[c.Util.CD_OPTION_TYPE] == 'call')][c.Util.PCT_IMPLIED_VOL].values[0]
        iv_put_2 = df_iv_atm_2[(df_iv_atm_2[c.Util.DT_DATE] == dt_date) & (df_iv_atm_2[c.Util.CD_OPTION_TYPE] == 'put')][c.Util.PCT_IMPLIED_VOL].values[0]
        iv_2 = (iv_call_2 + iv_put_2) / 2
        iv_call_3 = df_iv_atm_3[
            (df_iv_atm_3[c.Util.DT_DATE] == dt_date) & (df_iv_atm_3[c.Util.CD_OPTION_TYPE] == 'call')][c.Util.PCT_IMPLIED_VOL].values[0]
        iv_put_3 = df_iv_atm_3[(df_iv_atm_3[c.Util.DT_DATE] == dt_date) & (df_iv_atm_3[c.Util.CD_OPTION_TYPE] == 'put')][c.Util.PCT_IMPLIED_VOL].values[0]
        iv_3 = (iv_call_3 + iv_put_3) / 2
        iv_term_structure.append({'date': dt_date, 'iv1': iv_1, 'iv2': iv_2, 'iv3': iv_3})
    df = pd.DataFrame(iv_term_structure)
    df.to_csv('../data/' + name_code + '_iv_term_structure.csv')


def LLKSR_analysis(dt_start, df_iv, df_future_c1_daily, name_code):
    df_iv = df_iv[df_iv[c.Util.DT_DATE] >= dt_start].dropna().reset_index(drop=True)
    iv_atm = df_iv['average_iv']
    dates = list(df_iv[c.Util.DT_DATE])
    df_estimated_iv = pd.DataFrame()
    tmp = LLKSR(iv_atm, 5)
    df_estimated_iv['LLKSR_iv_5'] = LLKSR(iv_atm, 5)
    df_estimated_iv['LLKSR_iv_10'] = LLKSR(iv_atm, 10)
    df_estimated_iv['LLKSR_iv_20'] = LLKSR(iv_atm, 20)
    # df_estimated_iv['MA_iv_5'] = c.Statistics.moving_average(iv_atm, n=5)

    df_future_c1_daily.loc[:, 'histvol_20'] = Histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE], n=20)
    df_histvol = df_future_c1_daily[df_future_c1_daily[c.Util.DT_DATE] >= dt_start].dropna().reset_index(drop=True)

    f1 = pu.plot_line_chart(dates, [list(df_estimated_iv['LLKSR_iv_5']), list(df_estimated_iv['LLKSR_iv_10']),
                                    list(df_estimated_iv['LLKSR_iv_20'])],
                            ['隐含波动率LLKSR趋势线 (h=5)', '隐含波动率LLKSR趋势线 (h=10)', '隐含波动率LLKSR趋势线 (h=20)'])
    f2 = pu.plot_line_chart(dates, [list(df_estimated_iv['LLKSR_iv_10']), list(df_histvol['histvol_20'])],
                            ['隐含波动率LLKSR趋势线 (h=10)', '历史波动率LLKSR趋势线 (h=10)'])
    f3 = pu.plot_line_chart(dates, [list(df_estimated_iv['LLKSR_iv_10']), list(iv_atm)],
                       ['隐含波动率LLKSR趋势线 (h=10)', 'iv_atm'])
    f4 = pu.plot_line_chart(dates, [list(df_iv['iv_call']), list(df_iv['iv_put'])],
                            ['iv call', 'iv put'])
    f1.savefig('../data/' + name_code + '_iv_LLKSRs.png', dpi=300, format='png', bbox_inches='tight')
    f2.savefig('../data/' + name_code + '_iv_hv_LLKSR.png', dpi=300, format='png', bbox_inches='tight')
    f3.savefig('../data/' + name_code + '_iv_LLKSR.png', dpi=300, format='png', bbox_inches='tight')
    f4.savefig('../data/' + name_code + '_iv_call_put.png', dpi=300, format='png', bbox_inches='tight')

""""""
name_code = c.Util.STR_M
core_id = 'm_1901'
end_date = datetime.date(2018, 9, 4)
last_week = datetime.date(2018, 8, 31)
start_date = datetime.date(2018, 1, 1)
dt_histvol = start_date - datetime.timedelta(days=200)
min_holding = 5

""""""
df_res = pd.DataFrame()
pu = PlotUtil()
""""""

df_metrics = get_data.get_comoption_mktdata(start_date, end_date, name_code)
df_future_c1_daily = get_data.get_future_c1_by_option_daily(dt_histvol, end_date, name_code, min_holding)
df_iv_atm = get_data.get_iv_by_moneyness(start_date, end_date, name_code, nbr_moneyness=0)
df_iv_atm_call = df_iv_atm[df_iv_atm[c.Util.CD_OPTION_TYPE] == 'call']
df_iv_atm_put = df_iv_atm[df_iv_atm[c.Util.CD_OPTION_TYPE] == 'put']
df_iv = df_iv_atm_call[[c.Util.DT_DATE, c.Util.PCT_IMPLIED_VOL, c.Util.DT_MATURITY]].rename(
    columns={c.Util.PCT_IMPLIED_VOL: 'iv_call'})
df_iv = df_iv.join(df_iv_atm_put[[c.Util.DT_DATE, c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),
                   on=c.Util.DT_DATE, how='outer').rename(columns={c.Util.PCT_IMPLIED_VOL: 'iv_put'})
df_iv = df_iv.dropna().reset_index(drop=True)
df_iv.loc[:, 'average_iv'] = (df_iv.loc[:, 'iv_call'] + df_iv.loc[:, 'iv_put'])/2

d1 = max(df_metrics[c.Util.DT_DATE].values[0], df_future_c1_daily[c.Util.DT_DATE].values[0])


""" T-quote IV """
optionset = BaseOptionSet(df_metrics)
optionset.init()
optionset.go_to(end_date)
t_quote = optionset.get_T_quotes()
ivs_c = optionset.get_call_implied_vol_curve()
ivs_p = optionset.get_put_implied_vol_curve()
# ivs = ivs_c.join(ivs_p[c.Util.AMT_APPLICABLE_STRIKE,c.Util.PCT_IV_PUT].set_index(c.Util.AMT_APPLICABLE_STRIKE),on=c.Util.AMT_APPLICABLE_STRIKE)
ivs2 = pd.merge(ivs_c,ivs_p,how='left',on=c.Util.AMT_APPLICABLE_STRIKE)
""" 隐含波动率期限结构 """
dt_1 = last_week - datetime.timedelta(days=7)
dt_2 = last_week - datetime.timedelta(days=14)
if name_code == c.Util.STR_SR:
    implied_vol_term_structure([dt_2, dt_1, last_week, end_date])

""" PCR """
df_res = pcr(d1, name_code, df_res)

""" 历史波动率 """
df_res = hist_vol(d1, df_future_c1_daily, df_res)

""" 隐含波动率 """
df_res = implied_vol(d1, df_iv, df_res)
df_res = df_res.reset_index(drop=True)
df_res.to_csv('../data/' + name_code + '_data_report.csv')
"""当日成交持仓数据"""
trade_volume(end_date, last_week, df_metrics, name_code, core_id)


LLKSR_analysis(d1, df_iv, df_future_c1_daily, name_code)
plt.show()

""" 隐含波动率分析 """

""" 隐含波动率曲面 """
# optionset = BaseOptionSet(df_metrics)
# optionset.init()
# df_call_curve = optionset.get_call_implied_vol_curve(nbr_maturity=0)
# df_put_curve = optionset.get_put_implied_vol_curve(nbr_maturity=0)
# df_otm_curve = optionset.get_otm_implied_vol_curve(nbr_maturity=0)
#
# strikes = df_call_curve[[c.Util.AMT_APPLICABLE_STRIKE]]
# curve_call = df_call_curve[[c.Util.PCT_IMPLIED_VOL]]
# curve_put = df_put_curve[[c.Util.PCT_IMPLIED_VOL]]
# curve_otm = df_otm_curve[[c.Util.PCT_IV_OTM_BY_HTBR]]
# pu.plot_line_chart(strikes,[curve_call,curve_put],['curve_call','curve_put'])
# pu.plot_line_chart(strikes,[curve_otm],['curve_otm'])
