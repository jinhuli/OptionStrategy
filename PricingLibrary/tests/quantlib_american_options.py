# Copyright (C) 2004, 2005, 2006, 2007 StatPro Italia srl
#
# This file is part of QuantLib, a free-software/open-source library
# for financial quantitative analysts and developers - http://quantlib.org/
#
# QuantLib is free software: you can redistribute it and/or modify it under the
# terms of the QuantLib license.  You should have received a copy of the
# license along with this program; if not, please email
# <quantlib-dev@lists.sf.net>. The license is also available online at
# <http://quantlib.org/license.shtml>.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the license for more details.
from PricingLibrary.EngineQuantlib import QlBAW,QlBlackFormula,QlBinomial
from QuantLib import *
import datetime
from back_test.model.constant import QuantlibUtil,OptionType,OptionExerciseType

# global data
todaysDate = Date(17,May,1998)
Settings.instance().evaluationDate = todaysDate
settlementDate = Date(17,May,1998)
riskFreeRate = FlatForward(settlementDate, 0.06, ActualActual())

# option parameters
exercise = AmericanExercise(settlementDate, Date(17,May,1999))
payoff = PlainVanillaPayoff(Option.Put, 40.0)

# market data
underlying = SimpleQuote(36.0)
volatility = BlackConstantVol(todaysDate, China(), 0.20, ActualActual())
dividendYield = FlatForward(settlementDate, 0.00, ActualActual())

dt_settlement = QuantlibUtil.to_dt_date(settlementDate)
dt_maturity = QuantlibUtil.to_dt_date(Date(17,May,1999))
spot = 36.0
strike = 40.0
ql_baw = QlBAW(dt_settlement,dt_maturity,OptionType.PUT,OptionExerciseType.AMERICAN,spot,strike,rf=0.06)
ql_bonomial = QlBinomial(dt_settlement,dt_maturity,OptionType.PUT,OptionExerciseType.AMERICAN,spot,strike,rf=0.06)
# report
header = '%19s' % 'method' + ' |' + \
         ' |'.join(['%17s' % tag for tag in ['value',
                                            'estimated error',
                                            'actual error' ] ])
print(header)
print('-'*len(header))

refValue = None
def report(method, x, dx = None):
    e = '%.4f' % abs(x-refValue)
    x = '%.5f' % x
    if dx:
        dx = '%.4f' % dx
    else:
        dx = 'n/a'
    print('%19s' % method + ' |' + \
          ' |'.join(['%17s' % y for y in [x, dx, e] ]))

# good to go

process = BlackScholesMertonProcess(QuoteHandle(underlying),
                                    YieldTermStructureHandle(dividendYield),
                                    YieldTermStructureHandle(riskFreeRate),
                                    BlackVolTermStructureHandle(volatility))

option = VanillaOption(payoff, exercise)

refValue = 4.48667344
report('reference value',refValue)

# method: analytic

option.setPricingEngine(BaroneAdesiWhaleyEngine(process))
# report('Barone-Adesi-Whaley',option.NPV())
report('Barone-Adesi-Whaley',option.impliedVolatility(refValue,process))

# option.setPricingEngine(BjerksundStenslandEngine(process))
# report('Bjerksund-Stensland',option.NPV())

# method: finite differences
timeSteps = 800
gridPoints = 800

# option.setPricingEngine(FDAmericanEngine(process,timeSteps,gridPoints))
# report('finite differences',option.NPV())

# method: binomial
timeSteps = 800

# option.setPricingEngine(BinomialVanillaEngine(process,'jr',timeSteps))
# report('binomial (JR)',option.NPV())

option.setPricingEngine(BinomialVanillaEngine(process,'crr',timeSteps))
# report('binomial (CRR)',option.NPV())
report('binomial (CRR)',option.impliedVolatility(refValue,process))

# option.setPricingEngine(BinomialVanillaEngine(process,'eqp',timeSteps))
# report('binomial (EQP)',option.NPV())

# option.setPricingEngine(BinomialVanillaEngine(process,'trigeorgis',timeSteps))
# report('bin. (Trigeorgis)',option.NPV())

# option.setPricingEngine(BinomialVanillaEngine(process,'tian',timeSteps))
# report('binomial (Tian)',option.NPV())

# option.setPricingEngine(BinomialVanillaEngine(process,'lr',timeSteps))
# report('binomial (LR)',option.NPV())


report('binomial (self)',ql_bonomial.estimate_vol(refValue))
report('BAW (self)',ql_baw.estimate_vol(refValue))