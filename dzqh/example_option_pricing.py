from dzqh.EngineQuantlib import QlBlackFormula,QlBinomial,QlBAW
import dzqh.constant as c
import datetime
from dzqh.PlotUtil import PlotUtil
import numpy as np
import matplotlib.pyplot as plt


pu = PlotUtil()
mdt = datetime.date.today() + datetime.timedelta(days=30)

# Calculate Implied Volatility by Option Quote by BS Model
option_quote = 0.1
black_formula = QlBlackFormula(datetime.date.today(),mdt,c.OptionType.PUT,
                   spot=2.5,strike=2.5,rf=0.03,dividend_rate=0.0)
implied_vol=black_formula.estimate_vol(price=option_quote)
print('implied_vol BS : ',implied_vol)
print('delta BS: ',black_formula.Delta(implied_vol))
print('gamma BS : ',black_formula.Gamma(implied_vol))

# Calculate Option Price by Given Volatility by BS Model
vol = 0.33941
black_formula = QlBlackFormula(datetime.date.today(),mdt,c.OptionType.PUT,
                   spot=2.5,strike=2.5,vol=vol,rf=0.03,dividend_rate=0.0)
print('option price BS : ',black_formula.NPV())
print('delta BS : ',black_formula.Delta(vol))
print('gamma BS : ',black_formula.Gamma(vol))

# Plot Delta Graph
buy_write = c.BuyWrite.BUY # 期权头寸方向
strikes = np.arange(2.0,3.0,0.001)
deltas = []
gammas = []
for k in strikes:
    black_formula = QlBlackFormula(datetime.date.today(), mdt, c.OptionType.PUT,
                                   spot=2.5, strike=k, vol=vol,rf=0.03,dividend_rate=0.0)
    delta = black_formula.Delta(vol)*buy_write.value
    gamma = black_formula.Gamma(vol)*buy_write.value
    deltas.append(delta)
    gammas.append(gamma)

pu.plot_line_chart(strikes,[deltas],['delta'])
pu.plot_line_chart(strikes,[gammas],['gamma'])

plt.show()