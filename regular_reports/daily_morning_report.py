from sqlalchemy import *
import datetime
import pandas as pd
from data_access.db_tables import DataBaseTables as dbt
from Utilities import admin_util as admin
from Utilities.calculate import calculate_histvol
from regular_reports.report_util import *


# Eval Settings and Data
dt_date = datetime.date(2018, 7, 27)
dt_yesterday = dt_date - datetime.timedelta(days=1)
dt_start = datetime.date(2018, 1, 1)
dict_core_underlying = {}
text = ''
for namecode in ['m', 'sr']:
    str_trading_volume = 'total_trading_volume'
    str_holding_volume = 'total_holding_volume'
    str_id_underlying = 'id_underlying'
    df_volume_groupby = get_volume_groupby_id_option(admin.table_options_mktdata(), namecode, dt_start=dt_date,
                                                     dt_end=dt_date). \
        sort_values(by='total_trading_volume', ascending=False).reset_index(drop=True)
    df_volume_groupby_yeaterday = get_volume_groupby_id_option(admin.table_options_mktdata(), namecode,
                                                               dt_start=dt_yesterday,
                                                               dt_end=dt_yesterday). \
        sort_values(by='total_trading_volume', ascending=False).reset_index(drop=True)
    df_volume_groupby_id_type = get_volume_groupby_id_type(admin.table_options_mktdata(), namecode,
                                                           dt_start=dt_yesterday, dt_end=dt_date)
    id_c1 = df_volume_groupby.loc[0, 'id_underlying']
    id_c2 = df_volume_groupby.loc[1, 'id_underlying']
    dict_core_underlying.update({namecode: id_c1})
    df_c1 = get_volume_option(admin.table_options_mktdata(), id_c1, dt_date, dt_date)
    df_c1_call = df_c1[df_c1['cd_option_type'] == 'call'].sort_values(by=str_holding_volume,
                                                                      ascending=False).reset_index(
        drop=True)
    df_c1_put = df_c1[df_c1['cd_option_type'] == 'put'].sort_values(by=str_holding_volume, ascending=False).reset_index(
        drop=True)

    tradevolume_c1_today = df_volume_groupby.loc[0, 'total_trading_volume']
    holdvolume_c1_today = df_volume_groupby.loc[0, 'total_holding_volume']
    tradevolume_c1_yeaterday = \
        df_volume_groupby_yeaterday[df_volume_groupby_id_type['id_underlying'] == id_c1][str_trading_volume].values[0]
    holdvolume_c1_yeaterday = \
        df_volume_groupby_yeaterday[df_volume_groupby_id_type['id_underlying'] == id_c1][str_holding_volume].values[0]

    tradevolume_c2_today = df_volume_groupby.loc[2, str_trading_volume]
    holdvolume_c2_today = df_volume_groupby.loc[2, str_holding_volume]

    tradevolume_c1_c_today = df_volume_groupby_id_type[(df_volume_groupby_id_type['dt_date'] == dt_date) &
                                                       (df_volume_groupby_id_type['cd_option_type'] == 'call') &
                                                       (df_volume_groupby_id_type['id_underlying'] == id_c1)][
        str_trading_volume].values[0]
    tradevolume_c1_p_today = df_volume_groupby_id_type[(df_volume_groupby_id_type['dt_date'] == dt_date) &
                                                       (df_volume_groupby_id_type['cd_option_type'] == 'put') &
                                                       (df_volume_groupby_id_type['id_underlying'] == id_c1)][
        str_trading_volume].values[0]
    holdvolume_c1_c_today = df_volume_groupby_id_type[(df_volume_groupby_id_type['dt_date'] == dt_date) &
                                                      (df_volume_groupby_id_type['cd_option_type'] == 'call') &
                                                      (df_volume_groupby_id_type['id_underlying'] == id_c1)][
        str_holding_volume].values[0]
    holdvolume_c1_p_today = df_volume_groupby_id_type[(df_volume_groupby_id_type['dt_date'] == dt_date) &
                                                      (df_volume_groupby_id_type['cd_option_type'] == 'put') &
                                                      (df_volume_groupby_id_type['id_underlying'] == id_c1)][
        str_holding_volume].values[0]
    tradevolume_c2_c_today = df_volume_groupby_id_type[(df_volume_groupby_id_type['dt_date'] == dt_date) &
                                                       (df_volume_groupby_id_type['cd_option_type'] == 'call') &
                                                       (df_volume_groupby_id_type['id_underlying'] == id_c2)][
        str_trading_volume].values[0]
    tradevolume_c2_p_today = df_volume_groupby_id_type[(df_volume_groupby_id_type['dt_date'] == dt_date) &
                                                       (df_volume_groupby_id_type['cd_option_type'] == 'put') &
                                                       (df_volume_groupby_id_type['id_underlying'] == id_c2)][
        str_trading_volume].values[0]

    max_hold_amt_call_1st = df_c1_call.loc[0, str_holding_volume]
    max_hold_amt_put_1st = df_c1_put.loc[0, str_holding_volume]
    max_hold_k_call_1st = df_c1_call.loc[0, 'amt_strike']
    max_hold_k_put_1st = df_c1_put.loc[0, 'amt_strike']
    max_hold_amt_call_2nd = df_c1_call.loc[1, str_holding_volume]
    max_hold_amt_put_2nd = df_c1_put.loc[1, str_holding_volume]
    max_hold_k_call_2nd = df_c1_call.loc[1, 'amt_strike']
    max_hold_k_put_2nd = df_c1_put.loc[1, 'amt_strike']

    # refer here http://www.runoob.com/python/att-string-format.html for format usage
    text_1 = '{}年{}月{}日，{}期权{}合约总成交量为{:.0f}手，较前一交易日{}{:.0f}手，总持仓量为{:.0f}手，较前一交易日{}{:.0f}手。' \
             '其中，认购期权总成交量为{:.0f}手，认沽期权总成交量为{:.0f}手，成交量认沽认购比（PCR）为{:.2f}，' \
             '认购期权总持仓量为{:.0f}手,认沽期权总持仓量为{:.0f}手，' \
             '持仓量认沽认购比（PCR）为{:.2f}。此外{}合约当日认购与认沽合约总成交{:.0f}手, 总持仓为{:.0f}手。' \
             '最大持仓点位方面，{}合约认沽期权最大持仓对应行权价{:.0f}点，其次为{:.0f}点，持仓量分别为{:.0f}与{:.0f}手，' \
             '认购期权最大持仓点位为{:.0f}点，其次为{:.0f}点，持仓量分别为{:.0f}与{:.0f}手。\n \n'.format(
        dt_date.year,
        dt_date.month,
        dt_date.day,
        fun_report_namecode(namecode),
        id_c1[-4:],
        tradevolume_c1_today,
        fun_report_compare(tradevolume_c1_today, tradevolume_c1_yeaterday),
        abs(tradevolume_c1_today - tradevolume_c1_yeaterday),
        holdvolume_c1_today,
        fun_report_compare(holdvolume_c1_today, holdvolume_c1_yeaterday),
        abs(holdvolume_c1_today - holdvolume_c1_yeaterday),
        tradevolume_c1_c_today,
        tradevolume_c1_p_today,
        tradevolume_c1_p_today / tradevolume_c1_c_today,
        holdvolume_c1_c_today,
        holdvolume_c1_p_today,
        holdvolume_c1_p_today / holdvolume_c1_c_today,
        id_c2[-4:],
        tradevolume_c2_today,
        holdvolume_c2_today,
        id_c1[-4:],
        max_hold_k_put_1st,
        max_hold_k_put_2nd,
        max_hold_amt_put_1st,
        max_hold_amt_put_2nd,
        max_hold_k_call_1st,
        max_hold_k_call_2nd,
        max_hold_amt_call_1st,
        max_hold_amt_call_2nd,
    )
    text += text_1

m_id_c1 = dict_core_underlying['m']
m_df_future_c1 = get_mktdata_future_c1(dt_start, dt_date, 'm')
m_df_future = get_mktdata_future(admin.table_futures_mktdata(), m_id_c1, dt_yesterday, dt_date)
m_dict_iv_call, m_dict_iv_put = iv_at_the_money(dt_date, dt_yesterday, m_id_c1, m_df_future)
m_iv_call_today = m_dict_iv_call[dt_date]
m_iv_put_today = m_dict_iv_put[dt_date]
m_iv_call_yesterday = m_dict_iv_call[dt_yesterday]
m_iv_put_yesterday = m_dict_iv_put[dt_yesterday]
m_hisvol_1M = list(calculate_histvol(m_df_future_c1['amt_close'], 20))[-1] * 100
m_hisvol_3M = list(calculate_histvol(m_df_future_c1['amt_close'], 60))[-1] * 100

sr_id_c1 = dict_core_underlying['sr']
sr_df_future_c1 = get_mktdata_future_c1(dt_start, dt_date, 'sr')
sr_df_future = get_mktdata_future(admin.table_futures_mktdata(), sr_id_c1, dt_yesterday, dt_date)
sr_dict_iv_call, sr_dict_iv_put = iv_at_the_money(dt_date, dt_yesterday, sr_id_c1, sr_df_future)
sr_iv_call_today = sr_dict_iv_call[dt_date]
sr_iv_put_today = sr_dict_iv_put[dt_date]
sr_iv_call_yesterday = sr_dict_iv_call[dt_yesterday]
sr_iv_put_yesterday = sr_dict_iv_put[dt_yesterday]
sr_hisvol_1M = list(calculate_histvol(sr_df_future_c1['amt_close'], 20))[-1] * 100
sr_hisvol_3M = list(calculate_histvol(sr_df_future_c1['amt_close'], 60))[-1] * 100

text_2 = '隐含波动率方面，豆粕期权{}合约隐含波动率较前一交易日整体{}，其中，认沽期权平值合约隐含波动率为{:.2f}%，' \
         '较上一交易日({:.2f}%){}约{:.1f}%，' \
         '认购期权平值合约隐含波动率为{:.2f}%，较上一交易日({:.2f}%){}约{:.1f}%。' \
         '期货主力合约近1M历史波动率为{:.2f}%，近3M历史波动率为{:.2f}%，' \
         '平值期权隐含波动率较近1M历史波动率{}，较近3M历史波动率{}{}。' \
         '白糖期权{}合约隐含波动率较前一交易日整体{}，平值合约隐含波动率为{:.2f}%，' \
         '较上一交易日({:.2f}%){}约{:.1f}%。期货主力合约近1M历史波动率为{:.2f}%，近3M历史波动率为{:.2f}%，' \
         '平值期权隐含波动率较近1M历史波动率{}，较近3M历史波动率{}{}。' \
         '\n \n'.format(
    m_id_c1[-4:],
    fun_report_compare2(m_iv_call_today + m_iv_put_today, m_iv_call_yesterday + m_iv_put_yesterday),
    m_iv_put_today,
    m_iv_put_yesterday,
    fun_report_compare(m_iv_put_today, m_iv_put_yesterday),
    abs(m_iv_put_today - m_iv_put_yesterday),
    m_iv_call_today,
    m_iv_call_yesterday,
    fun_report_compare(m_iv_call_today, m_iv_call_yesterday),
    abs(m_iv_call_today - m_iv_call_yesterday),
    m_hisvol_1M,
    m_hisvol_3M,
    fun_report_compare3((m_iv_call_today + m_iv_put_today) / 2.0, m_hisvol_1M),
    fun_report_compare4(fun_report_compare3((m_iv_call_today + m_iv_put_today) / 2.0, m_hisvol_1M),
                        fun_report_compare3((m_iv_call_today + m_iv_put_today) / 2.0, m_hisvol_3M)),
    fun_report_compare3((m_iv_call_today + m_iv_put_today) / 2.0, m_hisvol_3M),

    sr_id_c1[-4:],
    fun_report_compare2(sr_iv_call_today + sr_iv_put_today, sr_iv_call_yesterday + sr_iv_put_yesterday),
    sr_iv_put_today,
    sr_iv_put_yesterday,
    fun_report_compare(sr_iv_put_today, sr_iv_put_yesterday),
    abs(sr_iv_put_today - sr_iv_put_yesterday),
    sr_hisvol_1M,
    sr_hisvol_3M,
    fun_report_compare3((sr_iv_call_today + sr_iv_put_today) / 2.0, sr_hisvol_1M),
    fun_report_compare4(fun_report_compare3((sr_iv_call_today + sr_iv_put_today) / 2.0, sr_hisvol_1M),
                        fun_report_compare3((sr_iv_call_today + sr_iv_put_today) / 2.0, sr_hisvol_3M)),
    fun_report_compare3((sr_iv_call_today + sr_iv_put_today) / 2.0, sr_hisvol_3M),
)
text += text_2
print(text)
with open("../morning_report.txt", "w") as text_file:
    text_file.write(text)
