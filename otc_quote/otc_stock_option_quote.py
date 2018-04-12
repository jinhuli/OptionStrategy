import datetime
import QuantLib as ql


def otc_quote(evalDate, mdtDate, spot_price,strike, vol, rf=0.03, dividend_rate=0.0, n=3):
    optionType = ql.Option.Call
    calendar = ql.China()
    daycounter = ql.ActualActual()
    underlying = ql.SimpleQuote(spot_price)
    volatility = ql.SimpleQuote(vol)
    eval_date = ql.Date(evalDate.day, evalDate.month, evalDate.year)
    effectivedt = calendar.advance(eval_date, ql.Period(n, ql.Days))  # T+3日可开始行权
    maturitydt = ql.Date(mdtDate.day, mdtDate.month, mdtDate.year)
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
    # exercise1 = ql.EuropeanExercise(maturitydt)
    # european_option = ql.VanillaOption(payoff, exercise1)
    # european_option.setPricingEngine(ql.AnalyticEuropeanEngine(bsmprocess))
    # bs_price = european_option.NPV()

    ame_option.setPricingEngine(ql.BinomialVanillaEngine(bsmprocess, 'crr', 801))
    ame_price = ame_option.NPV()
    return ame_price

#######################################################################################
evalDate = datetime.date(2018, 4, 9)  # 估值日
mdtDate = datetime.date(2018, 5, 8)  # 到期日
n = 3  # 下单日后n个工作日可开始行权
spot_price = 48.83  # 标的收盘价
vol = 35.5 / 100  # 波动率
rf = 0.03
dividend_rate = 0.0
strike = spot_price  # 平值期权

print('=' * 100)
ame_price = otc_quote(evalDate, mdtDate, spot_price,strike, vol)
# print('volalility : ', round(volatility.value(), 2))
print("The theoretical american option price is ", ame_price, ' , ', round(100 * ame_price / spot_price, 2),
      '% of spot')
print('-' * 100)
