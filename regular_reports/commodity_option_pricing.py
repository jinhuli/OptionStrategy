from back_test.model.base_option_set import BaseOptionSet
from data_access import get_data
import back_test.model.constant as c
import datetime

""""""
name_code = c.Util.STR_M
core_id = 'm_1901'
end_date = datetime.date(2018,9,7)
last_week = datetime.date(2018, 8, 31)
start_date = last_week
dt_histvol = start_date - datetime.timedelta(days=200)
min_holding = 5


df_metrics = get_data.get_comoption_mktdata(start_date, end_date, name_code)

""" T-quote IV """
optionset = BaseOptionSet(df_metrics,rf=0)
optionset.init()
optionset.go_to(end_date)
dt_maturity = optionset.select_maturity_date(0,min_holding)
call_list, put_list = optionset.get_options_list_by_moneyness_mthd1(0,dt_maturity)
atm_call = optionset.select_higher_volume(call_list)
atm_put = optionset.select_higher_volume(put_list)
print('atm call iv: ',atm_call.get_implied_vol())
print('atm put iv: ',atm_put.get_implied_vol())
print('atm strike: ',atm_put.strike())

t_quote = optionset.get_T_quotes(dt_maturity)
ivs_c1 = optionset.get_call_implied_vol_curve(dt_maturity)
ivs_p1 = optionset.get_put_implied_vol_curve(dt_maturity)
ivs_c2 = optionset.get_call_implied_vol_curve_htbr(dt_maturity)
ivs_p2 = optionset.get_put_implied_vol_curve_htbr(dt_maturity)

iv_curve = optionset.get_implied_vol_curves(dt_maturity)
iv_curve_htbr = optionset.get_implied_vol_curves_htbr(dt_maturity)
iv_volume_weighted = optionset.get_volume_weighted_iv(dt_maturity)
iv_volume_weighted_htbr = optionset.get_volume_weighted_iv_htbr(dt_maturity)
htbr = optionset.get_htb_rate(dt_maturity)
print(iv_volume_weighted)
print(iv_volume_weighted_htbr)
print(htbr)
print(iv_curve_htbr)
""" Quantlib """
# global data
# todaysDate = QuantlibUtil.to_ql_date(end_date)
# Settings.instance().evaluationDate = todaysDate
# settlementDate = todaysDate
# maturityDate = QuantlibUtil.to_ql_date(datetime.date(2018,12,7))
# dt_settlement = QuantlibUtil.to_dt_date(settlementDate)
# dt_maturity = QuantlibUtil.to_dt_date(maturityDate)
# rf = 0.0
# spot = 3120.0
# strike = 3650.0
# riskFreeRate = FlatForward(settlementDate, rf, ActualActual())
# # option parameters
# exercise = AmericanExercise(settlementDate, maturityDate)
# payoff = PlainVanillaPayoff(Option.Put, strike)
#
# # market data
# refValue = 538.0
# underlying = SimpleQuote(spot)
# volatility = BlackConstantVol(todaysDate, China(), 0.20, ActualActual())
# dividendYield = FlatForward(settlementDate, 0.00, ActualActual())
#
# process = BlackScholesMertonProcess(QuoteHandle(underlying),
#                                     YieldTermStructureHandle(dividendYield),
#                                     YieldTermStructureHandle(riskFreeRate),
#                                     BlackVolTermStructureHandle(volatility))
#
# option = VanillaOption(payoff, exercise)
#
# ql_baw = QlBAW(dt_settlement,dt_maturity,OptionType.PUT,OptionExerciseType.AMERICAN,spot,strike,rf=rf)
# ql_bonomial = QlBinomial(dt_settlement,dt_maturity,OptionType.PUT,OptionExerciseType.AMERICAN,spot,strike,rf=rf)
#
# option.setPricingEngine(BaroneAdesiWhaleyEngine(process))
# print('BAW : ',option.impliedVolatility(refValue,process))
# print('BAW local : ',ql_baw.estimate_vol_ql(refValue))
# print('BAW local2 : ',ql_baw.estimate_vol(refValue))
#
# option.setPricingEngine(BinomialVanillaEngine(process,'crr',800))
# print('Binomial : ',option.impliedVolatility(refValue,process))
# print('Binomial local : ',ql_bonomial.estimate_vol_ql(refValue))
# print('Binomial local2 : ',ql_bonomial.estimate_vol(refValue))

