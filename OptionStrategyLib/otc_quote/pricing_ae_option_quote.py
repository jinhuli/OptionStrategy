import datetime
import QuantLib as ql
import numpy as np

def otc_quote(evalDate,  spot_price,strike, vol,optionType, mdtDate=None, T=None, rf=0.03, dividend_rate=0.0, n=3):

    calendar = ql.China()
    daycounter = ql.ActualActual()
    underlying = ql.SimpleQuote(spot_price)
    volatility = ql.SimpleQuote(vol)
    eval_date = ql.Date(evalDate.day, evalDate.month, evalDate.year)
    effectivedt = calendar.advance(eval_date, ql.Period(n, ql.Days))  # T+3日可开始行权
    if T != None:
        if T == '1M':
            maturitydt  = calendar.advance(eval_date, ql.Period(1, ql.Months))
        elif T == '2M':
            maturitydt = calendar.advance(eval_date, ql.Period(2, ql.Months))
        elif T == '3M':
            maturitydt = calendar.advance(eval_date, ql.Period(3, ql.Months))
        else:
            return '期限不支持！'
    elif mdtDate != None:
        maturitydt = ql.Date(mdtDate.day, mdtDate.month, mdtDate.year)
    else:
        return '缺少到期日！'
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
    ame_option.setPricingEngine(ql.BinomialVanillaEngine(bsmprocess, 'crr', 801))
    ame_price = ame_option.NPV()
    return ame_price


#######################################################################################
evalDate = datetime.date(2018, 5, 22)  # 估值日
mdtDate = datetime.date(2018, 8, 7)  # 到期日
spot_price = 2950  # 标的收盘价
# vol = 17.5 / 100.0  # 波动率16.6
vol = 21.5/100 # 22
rf = 0.05
# strike = 2700  # 平值期权
# optionType = ql.Option.Put
strike = 3150  # 平值期权
optionType = ql.Option.Call


print('=' * 100)
ame_price = otc_quote(evalDate, spot_price,strike, vol,optionType,mdtDate=mdtDate)
print(spot_price, " : ", ame_price)
print('-' * 100)
for (idx,spot) in enumerate([spot_price-200,spot_price-100,spot_price+100,spot_price+200]):
    ame_price = otc_quote(evalDate, spot,strike, vol,optionType,mdtDate=mdtDate)
    # print('volalility : ', round(volatility.value(), 2))
    print(spot, " : ", ame_price)
    # print('-' * 100)

