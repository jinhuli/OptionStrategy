from back_test.model.base_option_set import BaseOptionSet
from back_test.model.base_account import BaseAccount
from back_test.model.base_instrument import BaseInstrument
from back_test.model.base_option import BaseOption
from data_access import get_data
import back_test.model.constant as c
import datetime
import numpy as np
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import pandas as pd
import math

start_date = datetime.date(2015, 2, 1)
end_date = datetime.date(2018, 8, 31)
d1 = start_date
min_holding = 20
nbr_maturity = 1
slippage = 0
pct_underlying_invest = 1.0

df_metrics = get_data.get_50option_mktdata(start_date, end_date)
df_underlying = get_data.get_index_mktdata(start_date, end_date, c.Util.STR_INDEX_50ETF)

calendar = c.Calendar(sorted(df_underlying[c.Util.DT_DATE].unique()))
pu = PlotUtil()

df = pd.DataFrame()
d1 = calendar.firstBusinessDayNextMonth(d1)
d2 = d1 + datetime.timedelta(days=365)
while d2 <= end_date:
    df_metrics_1 = df_metrics[(df_metrics[c.Util.DT_DATE] >= d1) & (df_metrics[c.Util.DT_DATE] <= d2)].reset_index(
        drop=True)
    df_underlying_1 = df_underlying[
        (df_underlying[c.Util.DT_DATE] >= d1) & (df_underlying[c.Util.DT_DATE] <= d2)].reset_index(drop=True)
    df_underlying_with_alpha = df_underlying_1[[c.Util.DT_DATE, c.Util.ID_INSTRUMENT, c.Util.AMT_CLOSE]]

    df_underlying_with_alpha.loc[:, 'p_last'] = df_underlying_with_alpha[c.Util.AMT_CLOSE].shift()
    df_underlying_with_alpha.loc[:, 'r1'] = (df_underlying_with_alpha.loc[:, c.Util.AMT_CLOSE]-df_underlying_with_alpha.loc[:, 'p_last'])/df_underlying_with_alpha.loc[:, 'p_last'] + 0.1 / 252
    df_underlying_with_alpha.loc[:, 'close_alpha'] = None
    p0 = df_underlying_with_alpha.loc[0, c.Util.AMT_CLOSE]
    for (idx, r) in df_underlying_with_alpha.iterrows():
        if idx == 0:
            df_underlying_with_alpha.loc[idx, 'close_alpha'] = df_underlying_with_alpha.loc[0, c.Util.AMT_CLOSE]
        else:
            df_underlying_with_alpha.loc[idx, 'close_alpha'] = df_underlying_with_alpha.loc[idx - 1, 'close_alpha'] * (1+ df_underlying_with_alpha.loc[idx, 'r1'])

    df_underlying_with_alpha = df_underlying_with_alpha[
        [c.Util.DT_DATE, c.Util.ID_INSTRUMENT, c.Util.AMT_CLOSE, 'close_alpha']].rename(
        columns={c.Util.AMT_CLOSE: 'etf_close'})
    df_underlying_with_alpha = df_underlying_with_alpha.rename(columns={'close_alpha': c.Util.AMT_CLOSE})
    alpha = (df_underlying_with_alpha.loc[len(df_underlying_with_alpha) - 1, c.Util.AMT_CLOSE]-p0) / p0
    etf = (df_underlying_with_alpha.loc[len(df_underlying_with_alpha) - 1, 'etf_close']-p0) / p0
    df_underlying_with_alpha.loc[:, 'npv_50etf'] = df_underlying_with_alpha.loc[:, 'etf_close'] / p0
    df_underlying_with_alpha.loc[:, 'npv_50etf_alpha'] = df_underlying_with_alpha.loc[:, c.Util.AMT_CLOSE] / p0
    npv_alpha = df_underlying_with_alpha.loc[len(df_underlying_with_alpha) - 1, 'npv_50etf_alpha']
    npv_etf = df_underlying_with_alpha.loc[len(df_underlying_with_alpha) - 1, 'npv_50etf']
    alpha1 = np.log(npv_alpha)
    etf1 = np.log(npv_etf)
    print(d1, ' ', alpha - etf, alpha1-etf1)
    # print('alpha : ', alpha, npv_alpha)
    # print('etf : ', etf, npv_etf)
    d1 = calendar.firstBusinessDayNextMonth(d1)
    d2 = d1 + datetime.timedelta(days=365)
