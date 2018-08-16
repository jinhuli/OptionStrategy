import numpy as np
from matplotlib import pylab as plt
import pandas as pd
import back_test.model.constant as c
from Utilities.PlotUtil import PlotUtil
from OptionStrategyLib.VolatilityModel.kernel_density import kde_sklearn

pu = PlotUtil()
df_data = pd.read_csv('../../../data/df_data.csv')
# df_data = pd.read_csv('../../data/df_underlying.csv')
dates = list(df_data[c.Util.DT_DATE])
histvols = np.array(df_data[c.Util.AMT_HISTVOL+'_20'])
garman_klass_vols = np.array(df_data[c.Util.AMT_GARMAN_KLASS+'_20'])
parkinson_vols = np.array(df_data[c.Util.AMT_PARKINSON_NUMBER+'_20'])
logvols = np.log(histvols)

x_grid = np.linspace(min(histvols), max(histvols), 1000)

plt.figure(1)
pdf_cc = kde_sklearn(histvols, x_grid, bandwidth=0.03)
plt.plot(x_grid, pdf_cc,'r--', label='Estimate')
plt.hist(histvols, bins=30, normed=True, color=(0,.5,0,1), label='Histogram')

plt.figure(2)
pdf_gk = kde_sklearn(garman_klass_vols, x_grid, bandwidth=0.03)
plt.plot(x_grid, pdf_gk,'r--', label='Estimate')
plt.hist(garman_klass_vols, bins=30, normed=True, color=(0,.5,0,1), label='Histogram')


plt.figure(3)
pdf_p = kde_sklearn(parkinson_vols, x_grid, bandwidth=0.03)
plt.plot(x_grid, pdf_p,'r--', label='Estimate')
plt.hist(parkinson_vols, bins=30, normed=True, color=(0,.5,0,1), label='Histogram')

plt.figure(4)
pu.plot_line_chart(x_grid,[pdf_cc,pdf_gk,pdf_p],['colse-close','garman_klass','parkinson'])


plt.figure(5)

vols_1w = np.array(df_data[c.Util.AMT_HISTVOL+'_5'])
pdf_1w = kde_sklearn(vols_1w, x_grid, bandwidth=0.03)
vols_1m = np.array(df_data[c.Util.AMT_HISTVOL+'_20'])
pdf_1m = kde_sklearn(vols_1m, x_grid, bandwidth=0.03)
vols_3m = np.array(df_data[c.Util.AMT_HISTVOL+'_60'])
pdf_3m = kde_sklearn(vols_3m, x_grid, bandwidth=0.03)
pu.plot_line_chart(x_grid,[pdf_1w,pdf_1m,pdf_3m],['1W','1M','3M'])


plt.show()
