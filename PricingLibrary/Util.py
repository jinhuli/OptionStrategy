import datetime
import math
from OptionStrategyLib.OptionPricing.BlackCalculator import BlackCalculator
from back_test.BktUtil import BktUtil


class PricingUtil:

    @staticmethod
    def get_ttm(dt_eval, dt_maturity):
        N = (dt_maturity - dt_eval).total_seconds() / 60.0
        N365 = 365 * 1440.0
        ttm = N / N365
        return ttm

    @staticmethod
    def get_std(dt_eval, dt_maturity, annualized_vol):
        stdDev = annualized_vol * math.sqrt(PricingUtil.get_ttm(dt_eval, dt_maturity))
        return stdDev

    @staticmethod
    def get_discount(dt_eval, dt_maturity, rf):
        discount = math.exp(-rf * PricingUtil.get_ttm(dt_eval, dt_maturity))
        return discount

    @staticmethod
    def get_maturity_metrics(self, dt_date, spot, option):
        strike = option.strike
        if option.option_type == BktUtil().type_put:
            if strike > spot:
                delta = -1.0
            elif strike < spot:
                delta = 1.0
            else:
                delta = 0.5
            option_price = max(strike - spot, 0)
        else:
            if strike < spot:
                delta = -1.0
            elif strike > spot:
                delta = 1.0
            else:
                delta = 0.5
            option_price = max(spot - strike, 0)
        delta = delta
        option_price = option_price
        return delta, option_price


class Calendar:
    def leepDates(self, dt1, dt2):
        # swap dt1 and dt2 if dt1 is earlier than dt2
        if (dt1 - dt2).days < 0:
            tmp = dt2
            dt2 = dt1
            dt1 = tmp
        # dt1 > dt2
        year1 = dt1.year
        year2 = dt2.year
        daysWithoutLeap = (year1 + 1 - year2) * 365
        daysWithLeap = (datetime.date(year1 + 1, 1, 1) - datetime.date(year2, 1, 1)).days
        leapDays = daysWithLeap - daysWithoutLeap
        if self.isLeapYear(dt1.year) and (dt1 - datetime.date(year1, 2, 29)).days < 0:
            print((dt1 - datetime.date(year1, 2, 29)).days)
            leapDays -= 1
        if self.isLeapYear(dt2.year) and (dt2 - datetime.date(year2, 2, 29)).days > 0:
            leapDays -= 1
        return (dt1 - dt2).days - leapDays

    def isLeapYear(self, year):
        return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

        # d1 = datetime.date(2016,12,22)
        # d2 = datetime.date(2015,1,11)
        #
        # d3 = datetime.date(d1.year,d2.month,d2.day)
        #
        # _leapdates = (d1-d3).days-leepDates(d1,d3)
        # frac_part = (d1-d3).days/(365.0+_leapdates)
        # yearFrac = (d1.year-d2.year)+frac_part
        #
        # print((d1-d2).days)
        # print(leepDates(d1,d2))
        # print(_leapdates)
        # print(leepDates(d1,d2)/365)
        # print(yearFrac)
