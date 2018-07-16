import datetime
import QuantLib as ql


#######################################################################################
evalDate = datetime.date(2018, 4, 12)
# mdtDate = datetime.date(2018, 5, 13)
spot_price = 11.75
vol = 0.0
rf = 0.03
# dividend_rate = 0.22/48.83 # 4月份分红预案已出 每10股派2.2
dividend_rate = 0.0
# strike = spot_price
strike = spot_price
quote = 7.33/100
option_price = quote*spot_price
print('=' * 100)

#######################################################################################

optionType = ql.Option.Call
calendar = ql.China()
daycounter = ql.ActualActual()
underlying = ql.SimpleQuote(spot_price)
volatility = ql.SimpleQuote(vol)
eval_date = ql.Date(evalDate.day, evalDate.month, evalDate.year)
effectivedt = calendar.advance(eval_date, ql.Period(3, ql.Days))  # T+3日可开始行权
maturitydt = calendar.advance(eval_date, ql.Period(1, ql.Months))
ql.Settings.instance().evaluationDate = eval_date

exercise = ql.AmericanExercise(effectivedt, maturitydt)
payoff = ql.PlainVanillaPayoff(optionType, strike)
ame_option = ql.VanillaOption(payoff, exercise)
flat_vol_ts = ql.BlackVolTermStructureHandle(
    ql.BlackConstantVol(eval_date, calendar, ql.QuoteHandle(volatility), daycounter))
dividend_ts = ql.YieldTermStructureHandle(
    ql.FlatForward(eval_date, dividend_rate, daycounter))
yield_ts = ql.YieldTermStructureHandle(ql.FlatForward(eval_date, rf, daycounter))
bsmprocess = ql.BlackScholesMertonProcess(ql.QuoteHandle(underlying), dividend_ts, yield_ts, flat_vol_ts)

exercise1 = ql.EuropeanExercise(maturitydt)
european_option = ql.VanillaOption(payoff, exercise1)

ame_option.setPricingEngine(ql.BinomialVanillaEngine(bsmprocess, 'crr', 801))

european_option.setPricingEngine(ql.AnalyticEuropeanEngine(bsmprocess))

iv = ame_option.impliedVolatility(option_price, bsmprocess, 1.0e-3, 300, 0.05, 1.0)

print("The implied vol is ", iv)
print('-' * 100)
