import pandas as pd
from back_test.model.constant import Statistics as stats
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
import datetime

pu = PlotUtil()
df_spx = pd.read_excel('../../data/global_option_data_copied.xlsx', sheetname='spx')
df_spx['dt_date'] = df_spx['Dates'].apply(lambda x: x.date())
df_spx = df_spx.dropna().sort_values(by='dt_date').reset_index(drop=True)
df_spx['cboe_skew_p90'] = stats.percentile(df_spx['cboe_skew'], 252, 0.9)
df_spx['cboe_skew_1std'] = stats.moving_average(df_spx['cboe_skew'], 252) + stats.standard_deviation(
    df_spx['cboe_skew'].dropna(), 252)
df_spx['cboe_skew_2std'] = stats.moving_average(df_spx['cboe_skew'], 252) + 2 * stats.standard_deviation(
    df_spx['cboe_skew'].dropna(), 252)
df_spx['cboe_vix_1std'] = stats.moving_average(df_spx['cboe_vix'], 252) + stats.standard_deviation(
    df_spx['cboe_vix'].dropna(), 252)
df_spx['cboe_vix_2std'] = stats.moving_average(df_spx['cboe_vix'], 252) + 2 * stats.standard_deviation(
    df_spx['cboe_vix'].dropna(), 252)
df_spx['cboe_vix_p90'] = stats.percentile(df_spx['cboe_vix'], 252, 0.9)
df_spx = df_spx[df_spx['dt_date'] > datetime.date(2015, 1, 1)]
df_spx['cboe_vix_diff_1std'] = df_spx['cboe_vix'] - df_spx['cboe_vix_1std']
df_spx['cboe_vix_diff_2std'] = df_spx['cboe_vix'] - df_spx['cboe_vix_2std']
df_spx['cboe_skew_diff_1std'] = df_spx['cboe_skew'] - df_spx['cboe_skew_1std']
df_spx['cboe_skew_diff_2std'] = df_spx['cboe_skew'] - df_spx['cboe_skew_2std']
pu.plot_line_chart(df_spx['dt_date'].tolist(), [df_spx['cboe_skew_2std'], df_spx['cboe_skew']],
                   ['cboe_skew_2std', 'cboe_skew'])
pu.plot_line_chart(df_spx['dt_date'].tolist(), [df_spx['cboe_vix_2std'], df_spx['cboe_vix']],
                   ['cboe_vix_2std', 'cboe_vix'])

df_sse = pd.read_excel('../../data/global_option_data_copied.xlsx', sheetname='sse')
df_sse['dt_date'] = df_sse['Dates'].apply(lambda x: x.date())
df_sse = df_sse.dropna().sort_values(by='dt_date').reset_index(drop=True)
# df_sse['sse_skew_p90'] = stats.percentile(df_sse['sse_skew_10delta'], 100, 0.9)
df_sse['sse_skew_1std'] = stats.moving_average(df_sse['sse_skew_10delta'], 252) + stats.standard_deviation(
    df_sse['sse_skew_10delta'], 252)
df_sse['sse_skew_2std'] = stats.moving_average(df_sse['sse_skew_10delta'], 252) + 2 * stats.standard_deviation(
    df_sse['sse_skew_10delta'], 252)
df_sse['sse_10d_skew_diff_2std'] = df_sse['sse_skew_10delta'] - df_sse['sse_skew_2std']
df_sse['sse_10d_skew_diff_1std'] = df_sse['sse_skew_10delta'] - df_sse['sse_skew_1std']

# pu.plot_line_chart(df_sse['dt_date'].tolist(), [df_sse['sse_skew_p90'], df_sse['sse_skew_10delta']],
#                    ['skew 90 percentile', 'skew(10D)'])
pu.plot_line_chart(df_sse['dt_date'].tolist(), [df_sse['sse_skew_2std'], df_sse['sse_skew_10delta']],
                   ['sse_skew_2std', 'skew(10D)'])

df_50etf = pd.read_excel('../../data/indexes.xlsx')
df_50etf['dt_date'] = df_50etf['dates'].apply(lambda x: x.date())

df_50etf = df_50etf.dropna().sort_values(by='dt_date').reset_index(drop=True)
df_50etf['vix_p90'] = stats.percentile(df_50etf['vix'].dropna(), 252, 0.9)
df_50etf['ivix_2std'] = stats.moving_average(df_50etf['vix'], 252) + 2 * stats.standard_deviation(df_50etf['vix'], 252)
df_50etf['ivix_1std'] = stats.moving_average(df_50etf['vix'], 252) + stats.standard_deviation(df_50etf['vix'], 252)

pu.plot_line_chart(df_50etf['dt_date'].tolist(), [df_50etf['ivix_2std'], df_50etf['vix']],
                   ['ivix_2std', 'vix index'])
df_50etf['ivix_diff_1std'] = df_50etf['vix'] - df_50etf['ivix_1std']
df_50etf['ivix_diff_2std'] = df_50etf['vix'] - df_50etf['ivix_2std']

df_monitor = pd.merge(df_spx[['dt_date', 'cboe_vix_diff_1std', 'cboe_vix_diff_2std', 'cboe_skew_diff_1std', 'cboe_skew_diff_2std']],
                      df_sse[['dt_date', 'sse_10d_skew_diff_1std', 'sse_10d_skew_diff_2std']], how='outer', on='dt_date')
df_monitor = pd.merge(df_monitor,df_50etf[['dt_date','ivix_diff_1std','ivix_diff_2std','50ETF','iv_atm_avg']], how='outer', on='dt_date')
# plt.show()
print(df_monitor)

df_monitor.to_csv('../../data/df_monitor.csv')
