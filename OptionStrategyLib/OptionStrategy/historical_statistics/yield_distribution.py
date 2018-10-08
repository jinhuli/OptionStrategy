from data_access import get_data
import back_test.model.constant as c
import matplotlib
from matplotlib import pylab as plt
import datetime
import numpy as np
from OptionStrategyLib.VolatilityModel.kernel_density import kde_sklearn
from Utilities.PlotUtil import PlotUtil
from scipy import stats

pu = PlotUtil()
start_date = datetime.date(2010, 1, 1)
end_date = datetime.date.today()
dt_histvol = start_date

""" 50ETF option """
name_code = c.Util.STR_CU
name_code_index = c.Util.STR_INDEX_50SH
# df_future_c1_daily = get_data.get_dzqh_cf_c1_daily(dt_histvol, end_date, name_code)
df_future_c1_daily = get_data.get_mktdata_future_c1_daily(dt_histvol, end_date, name_code)
df_index = get_data.get_index_mktdata(start_date, end_date, name_code_index)

df_future_c1_daily[c.Util.AMT_YIELD] = np.log(df_future_c1_daily[c.Util.AMT_CLOSE]).diff(periods=20)
df_index[c.Util.AMT_YIELD] = np.log(df_index[c.Util.AMT_CLOSE]).diff(periods=20)
df_future_c1_daily = df_future_c1_daily.dropna()
df_index = df_index.dropna()

r_future = np.array(df_future_c1_daily[c.Util.AMT_YIELD])
r_index = np.array(df_index[c.Util.AMT_YIELD])
s = np.random.normal(0, 0.15, 1000)

m1 = np.mean(r_future)
sk1 = stats.skew(r_future)
std1 = np.std(r_future)
m2 = np.mean(r_index)
sk2 = stats.skew(r_index)
std2 = np.std(r_index)
print(name_code,m1,std1,sk1)
print(name_code_index,m2,std2,sk2)
plt.figure(1)
x_f = np.linspace(min(r_future), max(r_future), 1000)
mu, sigma = stats.norm.fit(r_index)
pdf_norm = stats.norm.pdf(x_f, mu, sigma)
pdf_f = kde_sklearn(r_future, x_f, bandwidth=0.02)
plt.plot(x_f, pdf_norm, 'r--', linewidth=2, label='正态分布')
plt.plot(x_f, pdf_f, 'black', label='kernel density')
plt.hist(r_future, bins=100, normed=True, facecolor="#8C8C8C", label='沪铜期货回报率分布（月）')
plt.legend()

plt.figure(2)
x_indx = np.linspace(min(r_index), max(r_index), 1000)

mu, sigma = stats.norm.fit(r_index)
pdf_norm = stats.norm.pdf(x_indx, mu, sigma)
pdf_indx = kde_sklearn(r_index, x_indx, bandwidth=0.02)
plt.plot(x_indx, pdf_norm, 'r--', linewidth=2, label='正态分布')
plt.plot(x_indx, pdf_indx, 'black', label='kernel density')
plt.hist(r_index, bins=100, density=True, facecolor="#8C8C8C", label='上证50指数回报率分布(月）')
# plt.hist(r_index, bins=100, density=True,facecolor="#8C8C8C", label='沪深300指数回报率分布(月）')
plt.legend()

# plt.figure(3)
# pu.plot_line_chart(x_indx,[pdf_f,pdf_indx],['IH月度回报率 kernel density','50ETF月度回报率 kernel density'])
plt.show()
