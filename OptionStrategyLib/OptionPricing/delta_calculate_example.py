from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator
import math
import datetime

hedge_date = datetime.date(2017,7,19)
maturitydt = datetime.date(2017,9,27)
spot = 2.702
rf = 0.030
params = [-0.0689813692309 , 4.57986004886 , -0.980043687933 , -0.617957974368 , 0.117468005221]
vols = {2.2: 0.2449897447635236, 2.25: 0.22837076995826824, 2.3: 0.21949149068217633, 2.35: 0.20985304700906196, 2.4: 0.2041719266214876, 2.45: 0.20033927445461783, 2.5: 0.19807488063669001, 2.55: 0.19085764979481112, 2.6: 0.19159774017588876, 2.65: 0.1919016701828232, 2.7: 0.1934624432856883, 2.75: 0.19404949435809657}

ttm = (maturitydt-hedge_date).days/365
discount = math.exp(-rf*ttm)
dS = 0.001
iscall = False

print('spot = ',spot)
print('='*100)
print("%10s %25s " % ("Strike","delta_constant_vol "))
print('-'*100)

for strike in vols.keys():
    vol = vols.get(strike)
    stdDev = vol * math.sqrt(ttm)
    forward = spot/discount
    black = BlackCalculator(strike,forward,stdDev,discount,iscall)

    delta = black.delta(spot)

    delta1 = round(delta, 6)
    print("%10s %25s" % (strike,delta1))
print('='*100)
