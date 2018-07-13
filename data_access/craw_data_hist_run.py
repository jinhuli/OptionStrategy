# encoding: utf-8

from sqlalchemy import create_engine, MetaData, Table, Column, TIMESTAMP
import datetime
import pandas as pd
from WindPy import w
import os
from data_access import db_utilities as du
from data_access import spider_api_dce as dce
from data_access import spider_api_sfe as sfe
from data_access import spider_api_czce as czce
from data_access.db_data_collection import DataCollection
from Utilities import admin_write_util as admin

w.start()

conn = admin.conn_mktdata()
options_mktdata_daily = admin.table_options_mktdata()
futures_mktdata_daily = admin.table_futures_mktdata()
futures_institution_positions = admin.table_futures_institution_positions()

conn_intraday = admin.conn_intraday()
equity_index_intraday = admin.table_index_mktdata_intraday()
option_mktdata_intraday = admin.table_option_mktdata_intraday()
index_daily = admin.table_indexes_mktdata()

dc = DataCollection()
#####################################################################################
# beg_date = datetime.date(2004, 1, 1)
# end_date = datetime.date(2004, 12, 31)

################################### 低频宏观经济指标 ##################################################
macro_dict = {
    "M5792266" : ["industrial_capacity_utilization_quarter", "工业产能利用率：当季值"],
    "M0061571" : ["industrial_added_value_month_chain_ratio", "工业增加值：环比：季调"],
    "M0000545" : ["industrial_added_value_month_yoy_ratio", "工业增加值：当月：同比"],
    "M0000011" : ["industrial_added_value_month_cumulative_yoy_ratio", "工业增加值：累计同比"],
    "M0041963" : ["industrial_added_value_year", "全部工业增加值：亿元"],
    "M0041964" : ["industrial_added_value_year_yoy_ratio", "全部工业增加值：同比（年）"],
    "M9003295" : ["industrial_added_value_above_scale_year_yoy_ratio", "规模以上工业增加值：同比（年）"],
    "M0000272" : ["fixed_asset_investment_month", "固定资产投资完成额：累计值:亿元（月）"],
    "M0000273" : ["fixed_asset_investment_month_cumulative_yoy_ratio", "固定资产投资完成额：累计同比（月）"],
    "M0061572" : ["fixed_asset_investment_month_chain_ratio", "固定资产投资完成额：环比：季调（月）"],
    "M5207654" : ["industrial_main_business_cost_rate", "工业企业：主营业务成本率：累计值（月）"],
    "M5207655" : ["industrial_main_business_profit_rate", "工业企业：主营业务收入利润率：累计值（月）"],
    "M0001384" : ["M2_month", "M2：亿元（月）"],
    "M0001385" : ["M2_month_yoy_ratio", "M2同比（月）"],
    "M5525763" : ["social_financing_stock_month_yoy_ratio", "社会融资规模存量同比（月）"],
    "M5525755" : ["social_financing_stock_month",  "社会融资规模存量：万亿元（月）"],
    "M5525756" : ["social_financing_stock_rmb_month", "社会融资规模存量：人民币贷款：万亿元（月）"],
    "M5541321" : ["social_financing_month", "社会融资规模：当月值：初值：亿元（月）"],
    "M5541322" : ["social_financing_rmb_month", "社会融资规模：新增人民币贷款：当月值：初值（月）"],
    "M0017126" : ["china_pmi", "PMI"],
    "M5207838" : ["china_pmi_services", "非制造业PMI：服务业"],
    "M0017127" : ["china_pmi_production", "PMI生产"],
    "M0017128" : ["china_pmi_new_orders", "PMI新订单"],
    "M0017129" : ["china_pmi_new_orders_export", "PMI新出口订单"],
    "M0000138" : ["caixin_pmi", "财新中国PMI"],
    "M0061603" : ["caixin_pmi_services", "财新中国服务业PMI：经营活动指数"],

}

res = w.edb("M5792266,M0061571,M0000545,M0000011,M0041963,M0041964,M9003295,"
            "M0000272,M0000273,M0061572,M5207654,M5207655,M0001384,M0001385,"
            "M5525763,M5525755,M5525756,M5541321,M5541322,M0017126,M5207838,"
            "M0017127,M0017128,M0017129,M0000138,M0061603",
            "2000-01-01", "2018-07-12","ShowBlank=-999.0")
print(res)

df_res = pd.DataFrame()
for i, code in enumerate(res.Codes):
    df = pd.DataFrame()
    id_instrument = macro_dict[code][0]
    df['amt_close'] = res.Data[i]
    df['id_instrument'] = id_instrument
    df['code_instrument'] = code
    df['name_instrument'] = macro_dict[code][1].encode('utf-8')
    df['dt_date'] = res.Times
    df['timestamp'] = datetime.datetime.now()
    df_res = pd.concat([df_res, df], axis=0, ignore_index=True)

df_res.to_sql('macro', con=admin.engine, if_exists='append', index=False)



################################### INTEREST RATES ##################################################

# 国债到期收益率
#
# codes_dict = {
#     "M1004136": "gov_bond_yield_to_maturity_0",
#     "M1004677": "gov_bond_yield_to_maturity_1M",
#     "M1004829": "gov_bond_yield_to_maturity_2M",
#     "S0059741": "gov_bond_yield_to_maturity_3M",
#     "S0059742": "gov_bond_yield_to_maturity_6M",
#     "S0059743": "gov_bond_yield_to_maturity_9M",
#     "S0059744": "gov_bond_yield_to_maturity_1Y",
#     "S0059745": "gov_bond_yield_to_maturity_2Y",
#     "S0059746": "gov_bond_yield_to_maturity_3Y",
#     "M0057946": "gov_bond_yield_to_maturity_4Y",
#     "S0059747": "gov_bond_yield_to_maturity_5Y",
#     "M0057947": "gov_bond_yield_to_maturity_6Y",
#     "S0059748": "gov_bond_yield_to_maturity_7Y",
#     "M1000165": "gov_bond_yield_to_maturity_8Y",
#     "M1004678": "gov_bond_yield_to_maturity_9Y",
#     "S0059749": "gov_bond_yield_to_maturity_10Y",
#     "S0059750": "gov_bond_yield_to_maturity_15Y",
#     "S0059751": "gov_bond_yield_to_maturity_20Y",
#     "S0059752": "gov_bond_yield_to_maturity_30Y",
#     "M1004711": "gov_bond_yield_to_maturity_40Y",
#     "M1000170": "gov_bond_yield_to_maturity_50Y",
#
#     "M1004153": "company_bond_yield_to_maturity_AAA_0",
#     "M1006941": "company_bond_yield_to_maturity_AAA_1M",
#     "M1004552": "company_bond_yield_to_maturity_AAA_3M",
#     "S0059770": "company_bond_yield_to_maturity_AAA_6M",
#     "M1006942": "company_bond_yield_to_maturity_AAA_9M",
#     "S0059771": "company_bond_yield_to_maturity_AAA_1Y",
#     "S0059772": "company_bond_yield_to_maturity_AAA_2Y",
#     "S0059773": "company_bond_yield_to_maturity_AAA_3Y",
#     "M0057986": "company_bond_yield_to_maturity_AAA_4Y",
#     "S0059774": "company_bond_yield_to_maturity_AAA_5Y",
#     "M0057987": "company_bond_yield_to_maturity_AAA_6Y",
#     "S0059775": "company_bond_yield_to_maturity_AAA_7Y",
#     "M1000375": "company_bond_yield_to_maturity_AAA_8Y",
#     "M1006943": "company_bond_yield_to_maturity_AAA_9Y",
#     "S0059776": "company_bond_yield_to_maturity_AAA_10Y",
#     "S0059777": "company_bond_yield_to_maturity_AAA_15Y",
#     "S0059778": "company_bond_yield_to_maturity_AAA_20Y",
#     "S0059779": "company_bond_yield_to_maturity_AAA_30Y",
#
#     "M1005085": "company_bond_yield_to_maturity_AAA_minus_0",
#     "M1006944": "company_bond_yield_to_maturity_AAA_minus_1M",
#     "M1005086": "company_bond_yield_to_maturity_AAA_minus_3M",
#     "M1005087": "company_bond_yield_to_maturity_AAA_minus_6M",
#     "M1006945": "company_bond_yield_to_maturity_AAA_minus_9M",
#     "M1005088": "company_bond_yield_to_maturity_AAA_minus_1Y",
#     "M1005089": "company_bond_yield_to_maturity_AAA_minus_2Y",
#     "M1005090": "company_bond_yield_to_maturity_AAA_minus_3Y",
#     "M1005091": "company_bond_yield_to_maturity_AAA_minus_4Y",
#     "M1005092": "company_bond_yield_to_maturity_AAA_minus_5Y",
#     "M1005093": "company_bond_yield_to_maturity_AAA_minus_6Y",
#     "M1005094": "company_bond_yield_to_maturity_AAA_minus_7Y",
#     "M1005095": "company_bond_yield_to_maturity_AAA_minus_8Y",
#     "M1006946": "company_bond_yield_to_maturity_AAA_minus_9Y",
#     "M1005096": "company_bond_yield_to_maturity_AAA_minus_10Y",
#     "M1005097": "company_bond_yield_to_maturity_AAA_minus_15Y",
#     "M1005098": "company_bond_yield_to_maturity_AAA_minus_20Y",
#     "M1005099": "company_bond_yield_to_maturity_AAA_minus_30Y",
#
#     "M1004155": "company_bond_yield_to_maturity_AA_plus_0",
#     "M1006947": "company_bond_yield_to_maturity_AA_plus_1M",
#     "M1004554": "company_bond_yield_to_maturity_AA_plus_3M",
#     "S0059842": "company_bond_yield_to_maturity_AA_plus_6M",
#     "M1006948": "company_bond_yield_to_maturity_AA_plus_9M",
#     "S0059843": "company_bond_yield_to_maturity_AA_plus_1Y",
#     "S0059844": "company_bond_yield_to_maturity_AA_plus_2Y",
#     "S0059845": "company_bond_yield_to_maturity_AA_plus_3Y",
#     "M0057982": "company_bond_yield_to_maturity_AA_plus_4Y",
#     "S0059846": "company_bond_yield_to_maturity_AA_plus_5Y",
#     "M0057983": "company_bond_yield_to_maturity_AA_plus_6Y",
#     "S0059847": "company_bond_yield_to_maturity_AA_plus_7Y",
#     "M1000401": "company_bond_yield_to_maturity_AA_plus_8Y",
#     "M1006949": "company_bond_yield_to_maturity_AA_plus_9Y",
#     "S0059848": "company_bond_yield_to_maturity_AA_plus_10Y",
#     "S0059849": "company_bond_yield_to_maturity_AA_plus_15Y",
#     "S0059850": "company_bond_yield_to_maturity_AA_plus_20Y",
#     "S0059851": "company_bond_yield_to_maturity_AA_plus_30Y",
#
#     "M1004157": "company_bond_yield_to_maturity_AA_0",
#     "M1006950": "company_bond_yield_to_maturity_AA_1M",
#     "M1004556": "company_bond_yield_to_maturity_AA_3M",
#     "S0059760": "company_bond_yield_to_maturity_AA_6M",
#     "M1006951": "company_bond_yield_to_maturity_AA_9M",
#     "S0059761": "company_bond_yield_to_maturity_AA_1Y",
#     "S0059762": "company_bond_yield_to_maturity_AA_2Y",
#     "S0059763": "company_bond_yield_to_maturity_AA_3Y",
#     "S0059764": "company_bond_yield_to_maturity_AA_5Y",
#     "M0057978": "company_bond_yield_to_maturity_AA_6Y",
#     "S0059765": "company_bond_yield_to_maturity_AA_7Y",
#     "M1000427": "company_bond_yield_to_maturity_AA_8Y",
#     "M1006952": "company_bond_yield_to_maturity_AA_9Y",
#     "S0059766": "company_bond_yield_to_maturity_AA_10Y",
#     "S0059767": "company_bond_yield_to_maturity_AA_15Y",
#     "S0059768": "company_bond_yield_to_maturity_AA_20Y",
#     "S0059769": "company_bond_yield_to_maturity_AA_30Y",
#
#     "M1005115": "company_bond_yield_to_maturity_AA_minus_0",
#     "M1006953": "company_bond_yield_to_maturity_AA_minus_1M",
#     "M1005116": "company_bond_yield_to_maturity_AA_minus_3M",
#     "M1005117": "company_bond_yield_to_maturity_AA_minus_6M",
#     "M1006954": "company_bond_yield_to_maturity_AA_minus_9M",
#     "M1005118": "company_bond_yield_to_maturity_AA_minus_1Y",
#     "M1005119": "company_bond_yield_to_maturity_AA_minus_2Y",
#     "M1005120": "company_bond_yield_to_maturity_AA_minus_3Y",
#     "M1005121": "company_bond_yield_to_maturity_AA_minus_4Y",
#     "M1005122": "company_bond_yield_to_maturity_AA_minus_5Y",
#     "M1005123": "company_bond_yield_to_maturity_AA_minus_6Y",
#     "M1005124": "company_bond_yield_to_maturity_AA_minus_7Y",
#     "M1005125": "company_bond_yield_to_maturity_AA_minus_8Y",
#     "M1006955": "company_bond_yield_to_maturity_AA_minus_9Y",
#     "M1005126": "company_bond_yield_to_maturity_AA_minus_10Y",
#     "M1005127": "company_bond_yield_to_maturity_AA_minus_15Y",
#     "M1005128": "company_bond_yield_to_maturity_AA_minus_20Y",
#     "M1005129": "company_bond_yield_to_maturity_AA_minus_30Y",
#
#     "M1004161": "company_bond_yield_to_maturity_A_plus_0",
#     "M1006956": "company_bond_yield_to_maturity_A_plus_1M",
#     "M1004560": "company_bond_yield_to_maturity_A_plus_3M",
#     "S0059889": "company_bond_yield_to_maturity_A_plus_6M",
#     "M1006957": "company_bond_yield_to_maturity_A_plus_9M",
#     "S0059890": "company_bond_yield_to_maturity_A_plus_1Y",
#     "S0059891": "company_bond_yield_to_maturity_A_plus_2Y",
#     "S0059892": "company_bond_yield_to_maturity_A_plus_3Y",
#     "M0057970": "company_bond_yield_to_maturity_A_plus_4Y",
#     "S0059893": "company_bond_yield_to_maturity_A_plus_5Y",
#     "M0057971": "company_bond_yield_to_maturity_A_plus_6Y",
#     "S0059894": "company_bond_yield_to_maturity_A_plus_7Y",
#     "M1000479": "company_bond_yield_to_maturity_A_plus_8Y",
#     "M1006958": "company_bond_yield_to_maturity_A_plus_9Y",
#     "S0059895": "company_bond_yield_to_maturity_A_plus_10Y",
#     "S0059896": "company_bond_yield_to_maturity_A_plus_15Y",
#     "S0059897": "company_bond_yield_to_maturity_A_plus_20Y",
#     "S0059898": "company_bond_yield_to_maturity_A_plus_30Y",
#
#     "M1004162": "company_bond_yield_to_maturity_A_0",
#     "M1006959": "company_bond_yield_to_maturity_A_1M",
#     "M1004561": "company_bond_yield_to_maturity_A_3M",
#     "M1000484": "company_bond_yield_to_maturity_A_6M",
#     "M1000485": "company_bond_yield_to_maturity_A_1Y",
#     "M1000486": "company_bond_yield_to_maturity_A_2Y",
#     "M1000487": "company_bond_yield_to_maturity_A_3Y",
#     "M1000488": "company_bond_yield_to_maturity_A_4Y",
#     "M1000489": "company_bond_yield_to_maturity_A_5Y",
#     "M1000490": "company_bond_yield_to_maturity_A_6Y",
#     "M1000491": "company_bond_yield_to_maturity_A_7Y",
#     "M1000492": "company_bond_yield_to_maturity_A_8Y",
#     "M1006961": "company_bond_yield_to_maturity_A_9Y",
#     "M1000493": "company_bond_yield_to_maturity_A_10Y",
#     "M1000494": "company_bond_yield_to_maturity_A_15Y",
#     "M1000495": "company_bond_yield_to_maturity_A_20Y",
#     "M1000496": "company_bond_yield_to_maturity_A_30Y",
#
#     "M1004830": "company_bond_yield_to_maturity_A_minus_0",
#     "M1006962": "company_bond_yield_to_maturity_A_minus_1M",
#     "M1004831": "company_bond_yield_to_maturity_A_minus_3M",
#     "M1004832": "company_bond_yield_to_maturity_A_minus_6M",
#     "M1006963": "company_bond_yield_to_maturity_A_minus_9M",
#     "M1004833": "company_bond_yield_to_maturity_A_minus_1Y",
#     "M1004834": "company_bond_yield_to_maturity_A_minus_2Y",
#     "M1004835": "company_bond_yield_to_maturity_A_minus_3Y",
#     "M1004836": "company_bond_yield_to_maturity_A_minus_4Y",
#     "M1004837": "company_bond_yield_to_maturity_A_minus_5Y",
#     "M1004838": "company_bond_yield_to_maturity_A_minus_6Y",
#     "M1004839": "company_bond_yield_to_maturity_A_minus_7Y",
#     "M1004840": "company_bond_yield_to_maturity_A_minus_8Y",
#     "M1006964": "company_bond_yield_to_maturity_A_minus_9Y",
#     "M1004841": "company_bond_yield_to_maturity_A_minus_10Y",
#     "M1004842": "company_bond_yield_to_maturity_A_minus_15Y",
#     "M1004843": "company_bond_yield_to_maturity_A_minus_20Y",
#     "M1004844": "company_bond_yield_to_maturity_A_minus_30Y",
#
#     "M0041652" : "interbank_pledge_style_repo_1D",
#     "M0041653" : "interbank_pledge_style_repo_7D",
#     "M0041654" : "interbank_pledge_style_repo_14D",
#     "M0041655" : "interbank_pledge_style_repo_21D",
#     "M0041656" : "interbank_pledge_style_repo_1M",
#     "M0041657" : "interbank_pledge_style_repo_2M",
#     "M0041658" : "interbank_pledge_style_repo_3M",
#     "M0041659" : "interbank_pledge_style_repo_4M",
#     "M0041660" : "interbank_pledge_style_repo_6M",
#     "M0041661" : "interbank_pledge_style_repo_9M",
#     "M0041662" : "interbank_pledge_style_repo_1Y"

# }
# #
# res = w.edb("M1004153,M1006941,M1004552,S0059770,M1006942,S0059771,S0059772,S0059773,M0057986,"
#             "S0059774,M0057987,S0059775,M1000375,M1006943,S0059776,S0059777,S0059778,S0059779,"
#             "M1005085,M1006944,M1005086,M1005087,M1006945,M1005088,M1005089,M1005090,M1005091,"
#             "M1005092,M1005093,M1005094,M1005095,M1006946,M1005096,M1005097,M1005098,M1005099,"
#             "M1004155,M1006947,M1004554,S0059842,M1006948,S0059843,S0059844,S0059845,M0057982,"
#             "S0059846,M0057983,S0059847,M1000401,M1006949,S0059848,S0059849,S0059850,S0059851,"
#             "M1004157,M1006950,M1004556,S0059760,M1006951,S0059761,S0059762,S0059763,S0059764,"
#             "M0057978,S0059765,M1000427,M1006952,S0059766,S0059767,S0059768,S0059769,M1005115,"
#             "M1006953,M1005116,M1005117,M1006954,M1005119,M1005120,M1005121,M1005122,M1005123,"
#             "M1005124,M1005125,M1006955,M1005126,M1005127,M1005128,M1005129,M1004161,M1006956,"
#             "M1004560,S0059889,M1006957,S0059890,S0059891,S0059892,M0057970,S0059893,M0057971,"
#             "S0059894,M1006958,S0059895,S0059896,S0059897,S0059898,M1000479,M1004162,M1006959,"
#             "M1004561,M1000484,M1000485,M1000486,M1000487,M1000488,M1000489,M1000490,M1000491,"
#             "M1000492,M1006961,M1000493,M1000494,M1000495,M1000496,M1005118,M1004830,M1006962,"
#             "M1004831,M1004832,M1006963,M1004833,M1004834,M1004835,M1004836,M1004837,M1004838,"
#             "M1004839,M1004840,M1006964,M1004841,M1004842,M1004843,M1004844,"
#             "M0041652,M0041653,M0041654,M0041655,M0041656,M0041657,M0041658,M0041659,M0041660,M0041661,M0041662",
#             "2014-01-01", "2018-07-12", "Fill=Previous")
#
# df_res = pd.DataFrame()
# for i, code in enumerate(res.Codes):
#     df = pd.DataFrame()
#     id_instrument = codes_dict[code]
#     df['amt_close'] = res.Data[i]
#     df['id_instrument'] = id_instrument
#     df['code_instrument'] = code
#     df['dt_date'] = res.Times
#     df['timestamp'] = datetime.datetime.now()
#     df_res = pd.concat([df_res, df], axis=0, ignore_index=True)
#
# df_res.to_sql('interest_rates', con=admin.engine, if_exists='append', index=False)
#




# windcode = "510050.SH"
# id_instrument = 'index_50etf'
# db_data = dc.table_index().wind_data_index_hist(windcode, beg_date,end_date, id_instrument)
# try:
#     conn.execute(index_daily.insert(), db_data)
#     print('equity_index-50etf -- inserted into data base succefully')
# except Exception as e:
#     print(e)
#
#
# windcode = "000016.SH"
# id_instrument = 'index_50sh'
# db_data = dc.table_index().wind_data_index_hist(windcode, beg_date,end_date, id_instrument)
#
# try:
#     conn.execute(index_daily.insert(), db_data)
#     print('equity_index-50sh -- inserted into data base succefully')
# except Exception as e:
#     print(e)
#
#
# windcode = "000300.SH"
# id_instrument = 'index_300sh'
# db_data = dc.table_index().wind_data_index_hist(windcode, beg_date,end_date, id_instrument)
#
# try:
#     conn.execute(index_daily.insert(), db_data)
#     print('equity_index-300sh -- inserted into data base succefully')
# except Exception as e:
#     print(e)
#
#
# windcode = "000905.SH"
# id_instrument = 'index_500sh'
# db_data = dc.table_index().wind_data_index_hist(windcode, beg_date,end_date, id_instrument)
#
# try:
#     conn.execute(index_daily.insert(), db_data)
#     print('equity_index-500sh -- inserted into data base succefully')
# except Exception as e:
#     print(e)



# date_range = w.tdays(beg_date, end_date, "").Data[0]
# for dt in date_range:
#     date = dt.date()
#     dt_date = date.strftime("%Y-%m-%d")
#     print(dt_date)
#     # #####################table_options_mktdata_daily######################################
#     # # wind 50ETF option
#     # res = options_mktdata_daily.select((options_mktdata_daily.c.dt_date == dt_date)
#     #                                    & (options_mktdata_daily.c.name_code == '50etf')).execute()
#     # if res.rowcount == 0:
#     #
#     #     try:
#     #         db_data = dc.table_options().wind_data_50etf_option(dt_date)
#     #         if len(db_data) == 0: print('no data')
#     #         conn.execute(options_mktdata_daily.insert(), db_data)
#     #         print('wind 50ETF option -- inserted into data base succefully')
#     #     except Exception as e:
#     #         print(e)
#     # else:
#     #     print('wind 50ETF option -- already exists')
#     #
#     # # dce option data
#     # res = options_mktdata_daily.select((options_mktdata_daily.c.dt_date == dt_date)
#     #                                    & (options_mktdata_daily.c.cd_exchange == 'dce')).execute()
#     # if res.rowcount == 0:
#     #     # dce option data (type = 1), day
#     #     try:
#     #         ds = dce.spider_mktdata_day(date, date, 1)
#     #         for dt in ds.keys():
#     #             data = ds[dt]
#     #             if len(data) == 0: continue
#     #             db_data = dc.table_options().dce_day(dt, data)
#     #             if len(db_data) == 0: continue
#     #             try:
#     #                 conn.execute(options_mktdata_daily.insert(), db_data)
#     #                 print('dce option data -- inserted into data base succefully')
#     #             except Exception as e:
#     #                 print(dt)
#     #                 print(e)
#     #                 continue
#     #     except Exception as e:
#     #         print(e)
#     #     # dce option data (type = 1), night
#     #     try:
#     #         ds = dce.spider_mktdata_night(date, date, 1)
#     #         for dt in ds.keys():
#     #             data = ds[dt]
#     #             if len(data) == 0: continue
#     #             db_data = dc.table_options().dce_night(dt, data)
#     #             if len(db_data) == 0: continue
#     #             try:
#     #                 conn.execute(options_mktdata_daily.insert(), db_data)
#     #                 print('dce option data -- inserted into data base succefully')
#     #             except Exception as e:
#     #                 print(dt)
#     #                 print(e)
#     #                 continue
#     #     except Exception as e:
#     #         print(e)
#     # else:
#     #     print('dce option -- already exists')
#     #
#     # # czce option data
#     # res = options_mktdata_daily.select((options_mktdata_daily.c.dt_date == dt_date)
#     #                                    & (options_mktdata_daily.c.cd_exchange == 'czce')).execute()
#     # if res.rowcount == 0:
#     #     try:
#     #         ds = czce.spider_option(date, date)
#     #         for dt in ds.keys():
#     #             data = ds[dt]
#     #             if len(data) == 0: continue
#     #             db_data = dc.table_options().czce_daily(dt, data)
#     #             if len(db_data) == 0: continue
#     #             try:
#     #                 conn.execute(options_mktdata_daily.insert(), db_data)
#     #                 print('czce option data -- inserted into data base succefully')
#     #             except Exception as e:
#     #                 print(dt)
#     #                 print(e)
#     #                 continue
#     #     except Exception as e:
#     #         print(e)
#     # else:
#     #     print('czce option -- already exists')
#     #
#     # #####################futures_mktdata_daily######################################
#     # # equity index futures
#     # res = futures_mktdata_daily.select((futures_mktdata_daily.c.dt_date == dt_date)
#     #                                    & (futures_mktdata_daily.c.cd_exchange == 'cfe')).execute()
#     # if res.rowcount == 0:
#     #     try:
#     #         df = dc.table_future_contracts().get_future_contract_ids(dt_date)
#     #         for (idx_oc, row) in df.iterrows():
#     #             db_data = dc.table_futures().wind_index_future_daily(dt_date, row['id_instrument'], row['windcode'])
#     #             try:
#     #                 conn.execute(futures_mktdata_daily.insert(), db_data)
#     #                 print('equity index futures -- inserted into data base succefully')
#     #             except Exception as e:
#     #                 print(e)
#     #     except Exception as e:
#     #         print(e)
#     # else:
#     #     print('equity index futures -- already exists')
#     # # dce futures data
#     # res = futures_mktdata_daily.select((futures_mktdata_daily.c.dt_date == dt_date)
#     #                                    & (futures_mktdata_daily.c.cd_exchange == 'dce')).execute()
#     # if res.rowcount == 0:
#     #     # dce futures data (type = 0), day
#     #     try:
#     #         ds = dce.spider_mktdata_day(date, date, 0)
#     #         for dt in ds.keys():
#     #             data = ds[dt]
#     #             db_data = dc.table_futures().dce_day(dt, data)
#     #             if len(db_data) == 0: continue
#     #             try:
#     #                 conn.execute(futures_mktdata_daily.insert(), db_data)
#     #                 print('dce futures data -- inserted into data base succefully')
#     #             except Exception as e:
#     #                 print(dt)
#     #                 print(e)
#     #                 continue
#     #     except Exception as e:
#     #         print(e)
#     #     # dce futures data (type = 0), night
#     #     try:
#     #         ds = dce.spider_mktdata_night(date, date, 0)
#     #         for dt in ds.keys():
#     #             data = ds[dt]
#     #             db_data = dc.table_futures().dce_night(dt, data)
#     #             if len(db_data) == 0: continue
#     #             try:
#     #                 conn.execute(futures_mktdata_daily.insert(), db_data)
#     #                 print('dce futures data -- inserted into data base succefully')
#     #             except Exception as e:
#     #                 print(dt)
#     #                 print(e)
#     #                 continue
#     #     except Exception as e:
#     #         print(e)
#     # else:
#     #     print('dce future -- already exists')
#     #
#     # # sfe futures data
#     # res = futures_mktdata_daily.select((futures_mktdata_daily.c.dt_date == dt_date)
#     #                                    & (futures_mktdata_daily.c.cd_exchange == 'sfe')).execute()
#     # if res.rowcount == 0:
#     #     try:
#     #         ds = sfe.spider_mktdata(date, date)
#     #         for dt in ds.keys():
#     #             data = ds[dt]
#     #             db_data = dc.table_futures().sfe_daily(dt, data)
#     #             if len(db_data) == 0: continue
#     #             try:
#     #                 conn.execute(futures_mktdata_daily.insert(), db_data)
#     #                 print('sfe futures data -- inserted into data base succefully')
#     #             except Exception as e:
#     #                 print(dt)
#     #                 print(e)
#     #                 continue
#     #     except Exception as e:
#     #         print(e)
#     # else:
#     #     print('sfe future -- already exists')
#     #
#     # # czce futures data
#     # res = futures_mktdata_daily.select((futures_mktdata_daily.c.dt_date == dt_date)
#     #                                    & (futures_mktdata_daily.c.cd_exchange == 'czce')).execute()
#     # if res.rowcount == 0:
#     #     try:
#     #         ds = czce.spider_future(date, date)
#     #         for dt in ds.keys():
#     #             data = ds[dt]
#     #             db_data = dc.table_futures().czce_daily(dt, data)
#     #             # print(db_data)
#     #             if len(db_data) == 0:
#     #                 print('czce futures data -- no data')
#     #                 continue
#     #             try:
#     #                 conn.execute(futures_mktdata_daily.insert(), db_data)
#     #                 print('czce futures data -- inserted into data base succefully')
#     #             except Exception as e:
#     #                 print(dt)
#     #                 print(e)
#     #                 continue
#     #     except Exception as e:
#     #         print(e)
#     # else:
#     #     print('czce future -- already exists')
#
#     #####################index_mktdata_daily######################################
#     res = index_daily.select((index_daily.c.dt_date == dt_date) &
#                              (index_daily.c.id_instrument == 'index_50etf')).execute()
#     if res.rowcount == 0:
#         windcode = "510050.SH"
#         id_instrument = 'index_50etf'
#         db_data = dc.table_index().wind_data_index(windcode, dt_date, id_instrument)
#         try:
#             conn.execute(index_daily.insert(), db_data)
#             print('equity_index-50etf -- inserted into data base succefully')
#         except Exception as e:
#             print(e)
#     res = index_daily.select((index_daily.c.dt_date == dt_date) &
#                              (index_daily.c.id_instrument == 'index_50sh')).execute()
#     if res.rowcount == 0:
#         windcode = "000016.SH"
#         id_instrument = 'index_50sh'
#         db_data = dc.table_index().wind_data_index(windcode, dt_date, id_instrument)
#         try:
#             conn.execute(index_daily.insert(), db_data)
#             print('equity_index-50etf -- inserted into data base succefully')
#         except Exception as e:
#             print(e)
#     res = index_daily.select((index_daily.c.dt_date == dt_date) &
#                              (index_daily.c.id_instrument == 'index_300sh')).execute()
#     if res.rowcount == 0:
#         windcode = "000300.SH"
#         id_instrument = 'index_300sh'
#         db_data = dc.table_index().wind_data_index(windcode, dt_date, id_instrument)
#         try:
#             conn.execute(index_daily.insert(), db_data)
#             print('equity_index-50etf -- inserted into data base succefully')
#         except Exception as e:
#             print(e)
#     res = index_daily.select((index_daily.c.dt_date == dt_date) &
#                              (index_daily.c.id_instrument == 'index_500sh')).execute()
#     if res.rowcount == 0:
#         windcode = "000905.SH"
#         id_instrument = 'index_500sh'
#         db_data = dc.table_index().wind_data_index(windcode, dt_date, id_instrument)
#         try:
#             conn.execute(index_daily.insert(), db_data)
#             print('equity_index-500sh -- inserted into data base succefully')
#         except Exception as e:
#             print(e)
#     else:
#         print('index daily -- already exists')

# res = index_daily.select((index_daily.c.dt_date == dt_date) &
#                          (index_daily.c.id_instrument == 'index_cvix')).execute()
# if res.rowcount == 0:
#     windcode = "000188.SH"
#     id_instrument = 'index_cvix'
#     try:
#         db_data = dc.table_index().wind_data_index(windcode, dt_date, id_instrument)
#         conn.execute(index_daily.insert(), db_data)
#         print('equity_index-cvix -- inserted into data base succefully')
#     except Exception as e:
#         print(e)
# else:
#     print('index daily -- already exists')

# #####################equity_index_intraday######################################
# res = equity_index_intraday.select((equity_index_intraday.c.dt_datetime == dt_date + " 09:30:00") &
#                                    (equity_index_intraday.c.id_instrument == 'index_50etf')).execute()
# if res.rowcount == 0:
#     windcode = "510050.SH"
#     id_instrument = 'index_50etf'
#     db_data = dc.table_index_intraday().wind_data_equity_index(windcode, dt_date, id_instrument)
#     try:
#         conn_intraday.execute(equity_index_intraday.insert(), db_data)
#         print('equity_index_intraday-50etf -- inserted into data base succefully')
#     except Exception as e:
#         print(e)
# res = equity_index_intraday.select((equity_index_intraday.c.dt_datetime == dt_date + " 09:30:00") &
#                                    (equity_index_intraday.c.id_instrument == 'index_50sh')).execute()
# if res.rowcount == 0:
#     windcode = "000016.SH"
#     id_instrument = 'index_50sh'
#     db_data = dc.table_index_intraday().wind_data_equity_index(windcode, dt_date, id_instrument)
#     try:
#         conn_intraday.execute(equity_index_intraday.insert(), db_data)
#         print('equity_index_intraday-50sh -- inserted into data base succefully')
#     except Exception as e:
#         print(e)
# res = equity_index_intraday.select((equity_index_intraday.c.dt_datetime == dt_date + " 09:30:00") &
#                                    (equity_index_intraday.c.id_instrument == 'index_300sh')).execute()
# if res.rowcount == 0:
#     windcode = "000300.SH"
#     id_instrument = 'index_300sh'
#     db_data = dc.table_index_intraday().wind_data_equity_index(windcode, dt_date, id_instrument)
#     try:
#         conn_intraday.execute(equity_index_intraday.insert(), db_data)
#         print('equity_index_intraday-300sh -- inserted into data base succefully')
#     except Exception as e:
#         print(e)
# res = equity_index_intraday.select((equity_index_intraday.c.dt_datetime == dt_date + " 09:30:00") &
#                                    (equity_index_intraday.c.id_instrument == 'index_500sh')).execute()
# if res.rowcount == 0:
#     windcode = "000905.SH"
#     id_instrument = 'index_500sh'
#     db_data = dc.table_index_intraday().wind_data_equity_index(windcode, dt_date, id_instrument)
#     try:
#         conn_intraday.execute(equity_index_intraday.insert(), db_data)
#         print('equity_index_intraday-500sh -- inserted into data base succefully')
#     except Exception as e:
#         print(e)
# else:
#     print('equity index intraday -- already exists')
#
# ####################option_mktdata_intraday######################################
# df = dc.table_options().get_option_contracts(dt_date)
# for (idx_oc, row) in df.iterrows():
#     # print(row)
#     id_instrument = row['id_instrument']
#     res = option_mktdata_intraday.select((option_mktdata_intraday.c.dt_datetime == dt_date + " 09:30:00")&
#         (option_mktdata_intraday.c.id_instrument == id_instrument)).execute()
#     if res.rowcount == 0:
#         db_data = dc.table_option_intraday().wind_data_50etf_option_intraday(dt_date, row)
#         try:
#             conn_intraday.execute(option_mktdata_intraday.insert(), db_data)
#             print(idx_oc, ' option_mktdata_intraday -- inserted into data base succefully')
#         except Exception as e:
#             print(e)
#     else:
#         print(idx_oc,'option intraday -- already exists')

# ####################option_tick_data######################################
# # res = option_tick_data.select(option_tick_data.c.dt_datetime == dt_date+" 09:30:00").execute()
# # if res.rowcount == 0:
# df = dc.table_options().get_option_contracts(dt_date)
# # df = dc.table_option_tick().wind_option_chain(dt_date)
# for (idx_oc, row) in df.iterrows():
#     db_data = dc.table_option_tick().wind_50etf_option_tick(dt_date, row)
#     print(db_data)
#     try:
#         conn_intraday.execute(option_tick_data.insert(), db_data)
#         print(idx_oc, 'option_tick_data -- inserted into data base succefully')
#     except Exception as e:
#         print(e)
# # else:
# #     print('option_tick_data -- already exists')
# #####################future_tick_data######################################
# # equity index futures
# # res = future_tick_data.select(future_tick_data.c.dt_datetime == dt_date + " 09:30:00").execute()
# # if res.rowcount == 0:
# df = dc.table_future_contracts().get_future_contract_ids(dt_date)
# for (idx_oc, row) in df.iterrows():
#     db_data = dc.table_future_tick().wind_index_future_tick(dt_date, row['id_instrument'], row['windcode'])
#     try:
#         conn_intraday.execute(future_tick_data.insert(), db_data)
#         print(idx_oc, 'future_tick_data -- inserted into data base succefully')
#     except Exception as e:
#         print(e)
#         # else:
#         #     print('future_tick_data -- already exists')
