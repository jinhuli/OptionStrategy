from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator
import math
import datetime

hedge_date = datetime.date(2017, 7, 19)
maturitydt = hedge_date
spot = 2.702
rf = 0.030
strike = 2.2
vol = 0.2449897447635236


ttm = (maturitydt - hedge_date).days / 365
discount = math.exp(-rf * ttm)
dS = 0.001
iscall = False

print('spot = ', spot)
print('=' * 100)
print("%10s %25s " % ("Strike", "delta_constant_vol "))
print('-' * 100)



stdDev = vol * math.sqrt(ttm)
forward = spot / discount
black = BlackCalculator(strike, forward, stdDev, discount, iscall)

delta = black.Delta(spot)

print(delta)