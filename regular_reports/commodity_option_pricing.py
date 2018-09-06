from back_test.model.base_option_set import BaseOptionSet
from data_access import get_data
import back_test.model.constant as c
# import datetime
# from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol
# from Utilities.PlotUtil import PlotUtil
# import matplotlib.pyplot as plt
# from Utilities.timebase import LLKSR
# import Utilities.admin_util as admin
import pandas as pd
# from sqlalchemy import func
# from PricingLibrary.EngineQuantlib import QlBAW,QlBlackFormula,QlBinomial
# from QuantLib import *
import datetime
# from back_test.model.constant import QuantlibUtil,OptionType,OptionExerciseType

""""""
name_code = c.Util.STR_M
core_id = 'm_1901'
end_date = datetime.date(2018, 9, 4)
last_week = datetime.date(2018, 8, 31)
start_date = last_week
dt_histvol = start_date - datetime.timedelta(days=200)
min_holding = 5


df_metrics = get_data.get_comoption_mktdata(start_date, end_date, name_code)

""" T-quote IV """
optionset = BaseOptionSet(df_metrics)
optionset.init()
optionset.go_to(end_date)
t_quote = optionset.get_T_quotes()
ivs_c1 = optionset.get_call_implied_vol_curve()
ivs_p1 = optionset.get_put_implied_vol_curve()
ivs_c2 = optionset.get_call_implied_vol_curve_htbr()
ivs_p2 = optionset.get_put_implied_vol_curve_htbr()

ivs1 = pd.merge(ivs_c1,ivs_p1,how='inner',on=c.Util.AMT_APPLICABLE_STRIKE,suffixes=('','_y'))
ivs1 = ivs1[(ivs1[c.Util.PCT_IV_CALL]>0.05)&(ivs1[c.Util.PCT_IV_PUT]>0.05)].reset_index(drop=True)
# print(ivs1)
iv_vw = (sum(ivs1[c.Util.PCT_IV_CALL]*ivs1[c.Util.AMT_TRADING_VOLUME_CALL]) + sum(ivs1[c.Util.PCT_IV_PUT]*ivs1[c.Util.AMT_TRADING_VOLUME_PUT]))\
         /sum(ivs1[c.Util.AMT_TRADING_VOLUME_CALL]+ivs1[c.Util.AMT_TRADING_VOLUME_PUT])
print(iv_vw)
ivs_c2 = ivs_c2[ivs_c2[c.Util.PCT_IV_CALL_BY_HTBR]>0.05].reset_index(drop=True)
ivs_p2 = ivs_p2[ivs_p2[c.Util.PCT_IV_PUT_BY_HTBR]>0.05].reset_index(drop=True)
ivs2 = pd.merge(ivs_c2,ivs_p2,how='inner',on=c.Util.AMT_APPLICABLE_STRIKE,suffixes=('_call','_put'))

iv_vw2 = (sum(ivs2[c.Util.PCT_IV_CALL_BY_HTBR]*ivs2[c.Util.AMT_TRADING_VOLUME_CALL]) +
          sum(ivs2[c.Util.PCT_IV_PUT_BY_HTBR]*ivs2[c.Util.AMT_TRADING_VOLUME_PUT]))\
         /sum(ivs2[c.Util.AMT_TRADING_VOLUME_CALL]+ivs2[c.Util.AMT_TRADING_VOLUME_PUT])
print(iv_vw2)

iv_curve = optionset.get_implied_vol_curves()
iv_curve_htbr = optionset.get_put_implied_vol_curve_htbr()
iv_volume_weighted = optionset.get_volume_weighted_iv()
iv_volume_weighted_htbr = optionset.get_volume_weighted_iv_htbr()
htbr = optionset.get_htb_rate(nbr_maturity=0)
print(iv_volume_weighted)
print(iv_volume_weighted_htbr)
print(htbr)
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

