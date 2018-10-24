from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
import data_access.get_data as get_data
import back_test.model.constant as c
import datetime
import numpy as np
from OptionStrategyLib.OptionReplication.synthetic_option import SytheticOption
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd
from Utilities.timebase import LLKSR, KALMAN, LLT
from back_test.model.trade import Order
from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as histvol
pu = PlotUtil()
start_date = datetime.date(2015, 2, 1)
end_date = datetime.date.today()
dt_histvol = start_date - datetime.timedelta(days=90)
min_holding = 15  # 20 sharpe ratio较优
init_fund = c.Util.BILLION
slippage = 0
m = 1  # 期权notional倍数
cd_trade_price = c.CdTradePrice.CLOSE

""" 波动率数据：历史&隐含 """
df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_future_c1_daily = get_data.get_mktdata_future_c1_daily(dt_histvol, end_date, c.Util.STR_IH)
df_index = get_data.get_index_mktdata(dt_histvol,end_date,c.Util.STR_INDEX_50ETF)

df_future_c1_daily['ih_histvol_30'] = histvol.hist_vol(df_future_c1_daily[c.Util.AMT_CLOSE])
df_index['sh50_histvol_30'] = histvol.hist_vol(df_index[c.Util.AMT_CLOSE])

""" 隐含波动率 """
df_iv = get_data.get_iv_by_moneyness(dt_histvol,end_date,c.Util.STR_50ETF)
df_ivix = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='ivix']
df_iv_call = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='call']
df_iv_put = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put']
df_iv_htbr = df_iv[df_iv[c.Util.CD_OPTION_TYPE]=='put_call_htbr']
df_data = df_iv_call[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_call'})
df_data = df_data.join(df_iv_put[[c.Util.DT_DATE,c.Util.PCT_IMPLIED_VOL]].set_index(c.Util.DT_DATE),on=c.Util.DT_DATE,how='outer')\
    .rename(columns={c.Util.PCT_IMPLIED_VOL:'iv_put'})
df_data = df_data.dropna().reset_index(drop=True)
df_data.loc[:,'average_iv'] = (df_data.loc[:,'iv_call'] + df_data.loc[:,'iv_put'])/2
df_data['iv_htbr'] = df_iv_htbr.reset_index(drop=True)[c.Util.PCT_IMPLIED_VOL]
df_data['ivix'] = df_ivix.reset_index(drop=True)[c.Util.PCT_IMPLIED_VOL]
df_volatility = df_data[[c.Util.DT_DATE, 'average_iv','iv_htbr','ivix']]

df_volatility = pd.merge(df_volatility,df_future_c1_daily[[c.Util.DT_DATE,'ih_histvol_30']],on=c.Util.DT_DATE)
df_volatility = pd.merge(df_volatility,df_index[[c.Util.DT_DATE,'sh50_histvol_30']],on=c.Util.DT_DATE)
print(df_volatility)
df_volatility.to_csv('df_volatility.csv')
