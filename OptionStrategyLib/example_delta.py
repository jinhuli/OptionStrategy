import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from OptionStrategyLib.OptionPricing.BlackCalculator import EuropeanOption
from OptionStrategyLib.Util import PricingUtil

from Utilities.PlotUtil import PlotUtil
from back_test.deprecated.BktUtil import BktUtil

utl = BktUtil()
pricing_utl = PricingUtil()
strike = 3000
vol = 0.2
vol2 = 0.3
rf = 0.03
spotlist = np.arange(strike - 700, strike + 700, 10)
dt_date = datetime.datetime(2018, 3, 30)
dt_maturity = datetime.datetime(2018, 6, 30)
dt_maturity2 = datetime.datetime(2018, 4, 30)
Option = EuropeanOption(strike, dt_maturity, utl.type_put)
Option2 = EuropeanOption(strike, dt_maturity2, utl.type_put)
OptionCall = EuropeanOption(strike, dt_maturity, utl.type_call)
deltalist_1 = []
deltalist_2 = []
gammalist1 = []
gammalist2 = []
for spot in spotlist:
    delta = pricing_utl.get_blackcalculator(dt_date, spot, Option, rf, vol).Delta()
    # gamma = pricing_utl.get_blackcalculator(dt_date, spot, Option, rf, vol).Gamma()
    # gammalist1.append(gamma)
    deltalist_1.append(delta)
    delta2 = pricing_utl.get_blackcalculator(dt_date, spot, Option, 0.1, vol).Delta()
    # gamma2 = pricing_utl.get_blackcalculator(dt_date, spot, Option2, rf, vol).Gamma()
    deltalist_2.append(delta2)
    # gammalist2.append(gamma2)

plot_utl = PlotUtil()
plot_utl.plot_line_chart(spotlist, [deltalist_2,deltalist_1], ['Delta (rf=0.03)','Delta (rf=0.1)'])
# plot_utl.plot_line_chart(spotlist, [gammalist2,gammalist1], ['Gamma(T=1M)','Gamma(T=3M)'])
plt.show()
df = pd.DataFrame()
df['spot'] = spotlist
df['delta'] = deltalist_1
df.to_excel('../delta.xlsx')
# for spot in spotlist:
#     black1 = pricing_utl.get_blackcalculator(dt_date, spot, Option, rf, vol)
#     delta1 = black1.Delta()
#     gamma1 = black1.Gamma()
#     black2 = pricing_utl.get_blackcalculator(dt_date, spot, Option, rf, vol2)
#     delta2 = black2.Delta()
#     gamma2 = black2.Gamma()
#     gammalist1.append(gamma1)
#     deltalist_1.append(delta1)
#     deltalist_2.append(delta2)
#     gammalist2.append(gamma2)
#
#
# plot_utl = PlotUtil()
# plot_utl.plot_line_chart(spotlist, [deltalist_1,deltalist_2], ['Delta vol=0.2','Delta vol=0.3'])
# plot_utl.plot_line_chart(spotlist, [gammalist1,gammalist2], ['Gamma vol=0.2','Gamma vol=0.3'])
# plt.show()
