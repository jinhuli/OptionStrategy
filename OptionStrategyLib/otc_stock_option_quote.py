import datetime
import QuantLib as ql
import pandas as pd
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation

#######################################################################################
evalDate = datetime.date(2018,4,9)
mdtDate = datetime.date(2018,5,8)
spot_price = 48.83
vol = 21.0/100
rf = 0.03
# dividend_rate = 0.22/48.83 # 4月份分红预案已出 每10股派2.2
dividend_rate = 0.0
# strike = spot_price

res = []
for k_pct in [1.0,0.95,0.9,1.05,1.1]:
    strike = spot_price*k_pct
    print('='*100)
    print( 'strike is',strike, ',',100*k_pct,'% of spot')
    print('='*100)

    optionType = ql.Option.Call
    calendar = ql.China()
    daycounter = ql.ActualActual()
    underlying = ql.SimpleQuote(spot_price)
    volatility = ql.SimpleQuote(vol)
    eval_date = ql.Date(evalDate.day,evalDate.month,evalDate.year)
    effectivedt = calendar.advance(eval_date,ql.Period(3,ql.Days)) # T+3日可开始行权
    maturitydt = ql.Date(mdtDate.day,mdtDate.month,mdtDate.year)
    evaluation = Evaluation(eval_date,daycounter,calendar)

    #######################################################################################

    exercise = ql.AmericanExercise(effectivedt, maturitydt)
    payoff = ql.PlainVanillaPayoff(optionType, strike)
    ame_option = ql.VanillaOption(payoff, exercise)
    flat_vol_ts = ql.BlackVolTermStructureHandle(
        ql.BlackConstantVol(eval_date, calendar, ql.QuoteHandle(volatility), daycounter))
    dividend_ts = ql.YieldTermStructureHandle(
        ql.FlatForward(eval_date, dividend_rate, daycounter))
    yield_ts = ql.YieldTermStructureHandle(ql.FlatForward(eval_date, rf, daycounter))
    bsmprocess = ql.BlackScholesMertonProcess(ql.QuoteHandle(underlying), dividend_ts, yield_ts,flat_vol_ts)

    exercise1 = ql.EuropeanExercise(maturitydt)
    european_option = ql.VanillaOption(payoff, exercise1)

    ame_option.setPricingEngine(ql.BinomialVanillaEngine(bsmprocess, 'crr', 801))

    european_option.setPricingEngine(ql.AnalyticEuropeanEngine(bsmprocess))

    volatility.setValue(vol)
    ame_price = ame_option.NPV()
    bs_price = european_option.NPV()
    print('volalility : ',round(volatility.value(),2))
    print("The theoretical american option price is ", ame_price,' , ',round(100*ame_price/spot_price,2),'% of spot')
    print("The theoretical european option price is ", bs_price,' , ',round(100*bs_price/spot_price,2),'% of spot')
    print('-'*100)
    res.append({'1-k':k_pct,'2-vol':volatility.value(),'3-euro_option':bs_price,'4-ame_option':ame_price
                   ,'5-euro_option pct': round(100*bs_price/spot_price,2), '6-ame_option pct': round(100*ame_price/spot_price,2)})
    volatility.setValue(30.19/100) #中位数
    ame_price = ame_option.NPV()
    bs_price = european_option.NPV()

    print('volalility : ',round(volatility.value(),2))
    print("The theoretical american option price is ", ame_price,' , ',round(100*ame_price/spot_price,2),'% of spot')
    print("The theoretical european option price is ", bs_price,' , ',round(100*bs_price/spot_price,2),'% of spot')
    print('-'*100)
    res.append({'1-k':k_pct,'2-vol':volatility.value(),'3-euro_option':bs_price,'4-ame_option':ame_price
                   ,'5-euro_option pct': round(100*bs_price/spot_price,2), '6-ame_option pct': round(100*ame_price/spot_price,2)})

    volatility.setValue(35.53/100) #75分位数
    ame_price = ame_option.NPV()
    bs_price = european_option.NPV()

    print('volalility : ',round(volatility.value(),2))
    print("The theoretical american option price is ", ame_price,' , ',round(100*ame_price/spot_price,2),'% of spot')
    print("The theoretical european option price is ", bs_price,' , ',round(100*bs_price/spot_price,2),'% of spot')
    print('-'*100)
    res.append({'1-k':k_pct,'2-vol':volatility.value(),'3-euro_option':bs_price,'4-ame_option':ame_price
                   ,'5-euro_option pct': round(100*bs_price/spot_price,2), '6-ame_option pct': round(100*ame_price/spot_price,2)})


    volatility.setValue(52.00/100) #
    ame_price = ame_option.NPV()
    bs_price = european_option.NPV()
    print('volalility : ',round(volatility.value(),2))
    print("The theoretical american option price is ", ame_price,' , ',round(100*ame_price/spot_price,2),'% of spot')
    print("The theoretical european option price is ", bs_price,' , ',round(100*bs_price/spot_price,2),'% of spot')
    res.append({'1-k':k_pct,'2-vol':volatility.value(),'3-euro_option':bs_price,'4-ame_option':ame_price
                   ,'5-euro_option pct': round(100*bs_price/spot_price,2), '6-ame_option pct': round(100*ame_price/spot_price,2)})

df = pd.DataFrame(res)
df.to_csv('../save_figure/otc_quote.csv')