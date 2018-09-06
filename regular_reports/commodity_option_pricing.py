from back_test.model.base_option_set import BaseOptionSet
from data_access import get_data
import back_test.model.constant as c
import datetime
from OptionStrategyLib.VolatilityModel.historical_volatility import HistoricalVolatilityModels as Histvol
from Utilities.PlotUtil import PlotUtil
import matplotlib.pyplot as plt
from Utilities.timebase import LLKSR
import Utilities.admin_util as admin
import pandas as pd
from sqlalchemy import func
from PricingLibrary.EngineQuantlib import QlBAW,QlBlackFormula,QlBinomial
from QuantLib import *
import datetime
from back_test.model.constant import QuantlibUtil,OptionType,OptionExerciseType

""""""
name_code = c.Util.STR_M
core_id = 'm_1901'
end_date = datetime.date(2018, 9, 4)
last_week = datetime.date(2018, 8, 31)
start_date = datetime.date(2018, 1, 1)
dt_histvol = start_date - datetime.timedelta(days=200)
min_holding = 5


df_metrics = get_data.get_comoption_mktdata(start_date, end_date, name_code)

""" T-quote IV """
# optionset = BaseOptionSet(df_metrics)
# optionset.init()
# optionset.go_to(end_date)
# t_quote = optionset.get_T_quotes()
# ivs_c = optionset.get_call_implied_vol_curve()
# ivs_p = optionset.get_put_implied_vol_curve()
# # ivs = ivs_c.join(ivs_p[c.Util.AMT_APPLICABLE_STRIKE,c.Util.PCT_IV_PUT].set_index(c.Util.AMT_APPLICABLE_STRIKE),on=c.Util.AMT_APPLICABLE_STRIKE)
# ivs2 = pd.merge(ivs_c,ivs_p,how='left',on=c.Util.AMT_APPLICABLE_STRIKE)
# print(ivs2)

""" Quantlib """
# global data
todaysDate = QuantlibUtil.to_ql_date(end_date)
Settings.instance().evaluationDate = todaysDate
settlementDate = todaysDate
riskFreeRate = FlatForward(settlementDate, 0.06, ActualActual())
maturityDate = QuantlibUtil.to_ql_date(datetime.date(2018,12,7))
dt_settlement = QuantlibUtil.to_dt_date(settlementDate)
dt_maturity = QuantlibUtil.to_dt_date(maturityDate)
spot = 3117.0
strike = 3100.0
# option parameters
exercise = AmericanExercise(settlementDate, maturityDate)
payoff = PlainVanillaPayoff(Option.Call, strike)

# market data
# refValue = 99.0
refValue = 127.0
underlying = SimpleQuote(spot)
volatility = BlackConstantVol(todaysDate, China(), 0.20, ActualActual())
dividendYield = FlatForward(settlementDate, 0.00, ActualActual())

process = BlackScholesMertonProcess(QuoteHandle(underlying),
                                    YieldTermStructureHandle(dividendYield),
                                    YieldTermStructureHandle(riskFreeRate),
                                    BlackVolTermStructureHandle(volatility))

option = VanillaOption(payoff, exercise)

ql_baw = QlBAW(dt_settlement,dt_maturity,OptionType.PUT,OptionExerciseType.AMERICAN,spot,strike,rf=0.06)
ql_bonomial = QlBinomial(dt_settlement,dt_maturity,OptionType.CALL,OptionExerciseType.AMERICAN,spot,strike,rf=0.06)

option.setPricingEngine(BaroneAdesiWhaleyEngine(process))
print('Barone-Adesi-Whaley : ',option.impliedVolatility(refValue,process))
print('BAW local : ',ql_baw.estimate_vol_ql(refValue))
print('BAW local2 : ',ql_baw.estimate_vol(refValue))

option.setPricingEngine(BinomialVanillaEngine(process,'crr',800))
print('Binomial : ',option.impliedVolatility(refValue,process))

print('Binomial local : ',ql_bonomial.estimate_vol_ql(refValue))
