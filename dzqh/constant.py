from enum import Enum
import pandas as pd
import numpy as np
import math
import datetime
import typing
import QuantLib as ql


class FrequentType(Enum):
    DAILY = 1
    WEEKLY = 2
    MONTHLY = 3
    YEARLY = 4
    MINUTE = 5


class PricingType(Enum):
    OptionPlainEuropean = 1


class EngineType(Enum):
    AnalyticEuropeanEngine = 1


class LongShort(Enum):
    LONG = 1
    SHORT = -1


class MoneynessMethod(Enum):
    METHOD1 = 0
    METHOD2 = 1


class UnderlyingPriceType(Enum):
    CLOSE = 1
    OPEN = 2


class OptionType(Enum):
    CALL = 1
    PUT = -1


class OptionExerciseType(Enum):
    AMERICAN = 0
    EUROPEAN = 1


class DeltaBound(Enum):
    WHALLEY_WILLMOTT = 0
    NONE = -1


class BuyWrite(Enum):
    BUY = 1
    WRITE = -1


class TradeType(Enum):
    OPEN_LONG = 1
    OPEN_SHORT = 2
    CLOSE_LONG = -1
    CLOSE_SHORT = -2
    CLOSE_OUT = -3


class ExecuteType(Enum):
    EXECUTE_ALL_UNITS = 0
    EXECUTE_WITH_MAX_VOLUME = 1


class CdTradePrice(Enum):
    CLOSE = 0
    VOLUME_WEIGHTED = 1
    OPEN = 2


class Calendar(object):
    def __init__(self, date_list: typing.List[datetime.date]):
        self.date_list = sorted(date_list)
        self.max_year = self.date_list[-1].year
        self.date_map = {}
        self.init()

    """
    Initialize Calender with date_list
    {
        "2017": {
            "1": [datetime(2017,1,1), datetime(2017,1,2)],
            "3": [datetime(2017,3,1), datetime(2017,3,2)]
        }
    }
    """

    def init(self):
        for date in self.date_list:
            year_map = self.date_map.get(date.year, None)
            if year_map is None:
                year_map = {}
                self.date_map[date.year] = year_map
            month_list = year_map.get(date.month, None)
            if month_list is None:
                month_list = []
                year_map[date.month] = month_list
            month_list.append(date)
        for year, year_map in self.date_map.items():
            for month, month_list in year_map.items():
                year_map[month] = sorted(year_map[month])

    def next(self, dt):
        if dt < self.date_list[-1]:
            return self.date_list[self.date_list.index(dt) + 1]
        else:
            return

    def firstBusinessDayNextMonth(self, date: datetime.date) -> datetime.date:
        year = date.year
        month = date.month
        # Date is like 2017-12-3
        if month == 12:
            if year == self.max_year:
                raise ValueError("No available business in next month")
            else:
                return self.date_map.get(year + 1).get(1)[0]
        else:
            return self.date_map.get(year).get(month + 1)[0]

    def lastBusinessDayThisMonth(self, date: datetime.date) -> datetime.date:
        year = date.year
        month = date.month
        last_business_day_this_month = self.date_map.get(year).get(month)[-1]
        if date >= last_business_day_this_month:
            raise ValueError("No available business day after date {} this month".format(date))
        else:
            return last_business_day_this_month


class Util:
    """database column names"""
    # basic
    DT_DATE = 'dt_date'
    DT_DATETIME = 'dt_datetime'
    CODE_INSTRUMENT = 'code_instrument'
    ID_INSTRUMENT = 'id_instrument'
    ID_FUTURE = 'id_future'

    # option
    DT_MATURITY = 'dt_maturity'
    ID_UNDERLYING = 'id_underlying'
    CD_OPTION_TYPE = 'cd_option_type'
    NAME_CONTRACT_MONTH = 'name_contract_month'
    AMT_STRIKE = 'amt_strike'
    AMT_STRIKE_BEFORE_ADJ = 'amt_strike_before_adj'
    AMT_CLOSE = 'amt_close'
    AMT_OPEN = 'amt_open'
    AMT_HIGH = 'amt_high'
    AMT_LOW = 'amt_low'
    AMT_ADJ_OPTION_PRICE = 'amt_adj_option_price'
    AMT_OPTION_PRICE = 'amt_option_price'
    AMT_UNDERLYING_CLOSE = 'amt_underlying_close'
    AMT_UNDERLYING_OPEN_PRICE = 'amt_underlying_open_price'
    AMT_SETTLEMENT = 'amt_settlement'
    AMT_LAST_SETTLEMENT = 'amt_last_settlement'
    AMT_LAST_CLOSE = 'amt_last_close'
    NBR_MULTIPLIER = 'nbr_multiplier'
    AMT_HOLDING_VOLUME = 'amt_holding_volume'
    AMT_TRADING_VOLUME = 'amt_trading_volume'
    AMT_TRADING_VOLUME_CALL = 'amtl_trading_volume_call'
    AMT_TRADING_VOLUME_PUT = 'amt_trading_volume_put'
    AMT_TRADING_VALUE = 'amt_trading_value'
    AMT_MORNING_OPEN_15MIN = 'amt_morning_open_15min'
    AMT_MORNING_CLOSE_15MIN = 'amt_morning_close_15min'
    AMT_AFTERNOON_OPEN_15MIN = 'amt_afternoon_open_15min'
    AMT_AFTERNOON_CLOSE_15MIN = 'amt_afternoon_close_15min'
    AMT_MORNING_AVG = 'amt_morning_avg'
    AMT_AFTERNOON_AVG = 'amt_afternoon_avg'
    AMT_DAILY_AVG = 'amt_daily_avg'
    AMT_NEAREST_STRIKE = 'amt_nearest_strike'
    PCT_IMPLIED_VOL = 'pct_implied_vol'
    PCT_IV_OTM_BY_HTBR = 'pct_iv_by_htbr'
    PCT_IV_CALL_BY_HTBR = 'pct_iv_call_by_htbr'
    PCT_IV_PUT_BY_HTBR = 'pct_iv_put_by_htbr'
    PCT_IV_CALL = 'pct_iv_call'
    PCT_IV_PUT = 'pct_iv_put'
    AMT_DELTA = 'amt_delta'
    AMT_THETA = 'amt_theta'
    AMT_VEGA = 'amt_vega'
    AMT_RHO = 'amt_rho'
    AMT_CARRY = 'amt_carry'
    AMT_IV_ROLL_DOWN = 'amt_iv_roll_down'
    NBR_INVEST_DAYS = 'nbr_invest_days'
    RISK_FREE_RATE = 'risk_free_rate'
    AMT_APPLICABLE_STRIKE = 'amt_applicable_strike'
    AMT_APPLICABLE_MULTIPLIER = 'amt_applicable_multiplier'
    AMT_YIELD = 'amt_yield'
    AMT_HISTVOL = 'amt_hist_vol'
    AMT_PARKINSON_NUMBER = 'amt_parkinson_number'
    AMT_GARMAN_KLASS = 'amt_garman_klass'
    AMT_HEDHE_UNIT = 'amt_hedge_unit'
    AMT_CALL_QUOTE = 'amt_call_quote'
    AMT_PUT_QUOTE = 'amt_put_quote'
    AMT_TTM = 'amt_ttm'
    AMT_HTB_RATE = 'amt_HTB_rate'
    AMT_CLOSE_VOLUME_WEIGHTED = 'amt_close_volume_weighted'
    CD_CLOSE = 'cd_close'
    CD_CLOSE_VOLUME_WEIGHTED = 'cd_close_volume_weighted'
    NAME_CODE = 'name_code'
    STR_CALL = 'call'
    STR_PUT = 'put'
    STR_50ETF = '50etf'
    STR_INDEX_50ETF = 'index_50etf'
    STR_INDEX_50SH = 'index_50sh'
    STR_INDEX_300SH = 'index_300sh'
    STR_INDEX_300SH_TOTAL_RETURN = 'index_300sh_total_return'
    STR_M = 'm'
    STR_IH = 'ih'
    STR_IF = 'iF'
    STR_SR = 'sr'
    STR_ALL = 'all'
    STR_CU = 'cu'
    NAN_VALUE = -999.0

    LOW_FREQUENT = [FrequentType.DAILY, FrequentType.WEEKLY, FrequentType.MONTHLY, FrequentType.YEARLY]
    PRODUCT_COLUMN_LIST = [ID_INSTRUMENT, AMT_CLOSE, AMT_OPEN, AMT_SETTLEMENT, AMT_MORNING_OPEN_15MIN,
                           AMT_MORNING_CLOSE_15MIN, AMT_AFTERNOON_CLOSE_15MIN, AMT_MORNING_AVG, AMT_AFTERNOON_AVG,
                           AMT_DAILY_AVG, AMT_HOLDING_VOLUME, AMT_TRADING_VOLUME, AMT_LAST_SETTLEMENT,
                           AMT_LAST_CLOSE]
    INSTRUMENT_COLUMN_LIST = PRODUCT_COLUMN_LIST
    OPTION_COLUMN_LIST = PRODUCT_COLUMN_LIST + \
                         [NAME_CONTRACT_MONTH, AMT_STRIKE, AMT_STRIKE_BEFORE_ADJ, AMT_APPLICABLE_STRIKE, DT_MATURITY,
                          CD_OPTION_TYPE, AMT_OPTION_PRICE, AMT_ADJ_OPTION_PRICE, ID_UNDERLYING, AMT_UNDERLYING_CLOSE,
                          AMT_UNDERLYING_OPEN_PRICE, PCT_IMPLIED_VOL, NBR_MULTIPLIER, AMT_LAST_SETTLEMENT,
                          AMT_SETTLEMENT]
    NAME_CODE_159 = ['sr', 'm', 'ru']
    MAIN_CONTRACT_159 = [1, 5, 9, '01', '05', '09']
    NAME_CODE_1to12 = ['cu']
    # Trade
    LONG = 1
    SHORT = -1
    UUID = 'uuid'
    DT_TRADE = 'dt_trade'
    TRADE_TYPE = 'trade_type'
    TRADE_PRICE = 'trade_price'
    TRANSACTION_COST = 'transaction_cost'
    TRADE_UNIT = 'trade_unit'  # 绝对值
    TIME_SIGNAL = 'time_signal'
    OPTION_PREMIIUM = 'option_premium'
    CASH = 'cash'
    TRADE_MARGIN_CAPITAL = 'trade_margin_capital'
    TRADE_MARKET_VALUE = 'trade_market_value'  # 头寸市值
    TRADE_BOOK_VALUE = 'trade_book_value'  # 头寸规模（含多空符号），例如，空一手豆粕（3000点，乘数10）得到头寸规模为-30000，而建仓时点头寸市值为0。
    ABS_TRADE_BOOK_VALUE = 'abs_trade_book_value'
    TRADE_LONG_SHORT = 'long_short'
    AVERAGE_POSITION_COST = 'average_position_cost'  # 历史多次交易同一品种的平均成本(总头寸规模绝对值/unit)
    TRADE_REALIZED_PNL = 'realized_pnl'
    LAST_PRICE = 'last_price'
    POSITION_CURRENT_VALUE = 'position_current_value'  # 用于计算杠杆率，保证金交易的current value为零
    PORTFOLIO_MARGIN_CAPITAL = 'portfolio_margin_capital'
    PORTFOLIO_MARGIN_TRADE_SCALE = 'portfolio_margin_trade_scale'
    PORTFOLIO_TOTAL_SCALE = 'portfolio_total_scale'
    PORTFOLIO_TRADES_VALUE = 'portfolio_trades_value'
    PORTFOLIO_VALUE = 'portfolio_value'
    PORTFOLIO_NPV = 'npv'
    PORTFOLIO_UNREALIZED_PNL = 'unrealized_pnl'
    PORTFOLIO_LEVERAGE = 'portfolio_leverage'
    PORTFOLIO_SHORT_POSITION_SCALE = 'portfolio_short_position_scale'
    PORTFOLIO_LONG_POSITION_SCALE = 'portfolio_long_position_scale'
    MARGIN_UNREALIZED_PNL = 'margin_unrealized_pnl'
    NONMARGIN_UNREALIZED_PNL = 'nonmargin_unrealized_pnl'
    PORTFOLIO_DELTA = 'portfolio_delta'
    TURNOVER = 'turnover'
    DAILY_EXCECUTED_AMOUNT = 'daily_executed_amount'  # abs value
    BILLION = 1000000000.0
    TRADE_BOOK_COLUMN_LIST = [DT_DATE, TRADE_LONG_SHORT, TRADE_UNIT,
                              LAST_PRICE, TRADE_MARGIN_CAPITAL,
                              TRADE_BOOK_VALUE, AVERAGE_POSITION_COST,
                              TRADE_REALIZED_PNL, NBR_MULTIPLIER,
                              POSITION_CURRENT_VALUE, PORTFOLIO_UNREALIZED_PNL
                              ]  # ID_INSTRUMENR是df的index
    ACCOUNT_COLUMNS = [DT_DATE, CASH, PORTFOLIO_MARGIN_CAPITAL, PORTFOLIO_TRADES_VALUE,
                       PORTFOLIO_VALUE, PORTFOLIO_NPV, PORTFOLIO_UNREALIZED_PNL,
                       PORTFOLIO_LEVERAGE, TRADE_REALIZED_PNL,
                       PORTFOLIO_SHORT_POSITION_SCALE, PORTFOLIO_LONG_POSITION_SCALE,
                       MARGIN_UNREALIZED_PNL, NONMARGIN_UNREALIZED_PNL, PORTFOLIO_DELTA,
                       DAILY_EXCECUTED_AMOUNT, TURNOVER
                       ]
    DICT_FUTURE_MARGIN_RATE = {  # 合约价值的百分比
        'm': 0.05,
        'sr': 0.05,
        'if': 0.15,
        'ih': 0.15,
        'ic': 0.15,
        'index': 0.15
    }
    DICT_TRANSACTION_FEE = {  # 元/手
        'm': 3.0,
        'sr': 3.0,
        'if': None,
        'ih': None,
        'ic': None,
        "index": 0
    }
    DICT_OPTION_TRANSACTION_FEE_RATE = {  # 百分比
        "50etf": 0.0,
        "m": 0.0,
        "sr": 0.0,
        "cu": 0.0,
    }
    DICT_OPTION_TRANSACTION_FEE = {  # 元/手
        "50etf": 5.0,
        "m": 5.0,
        "sr": 5.0,
        "cu": 5.0,
    }
    DICT_TRANSACTION_FEE_RATE = {  # 百分比
        'm': None,
        'sr': None,
        'if': 6.9 / 10000.0,
        'ih': 6.9 / 10000.0,
        'ic': 6.9 / 10000.0,
        "index": None

    }
    DICT_CONTRACT_MULTIPLIER = {  # 合约乘数
        'm': 10,
        'sr': 10,
        'if': 300,
        'ih': 300,
        'ic': 200,
        'cu': 5,
        'index': 1
    }
    DICT_OPTION_CONTRACT_MULTIPLIER = {  # 合约乘数
        'm': 10,
        'sr': 10,
        STR_50ETF: 10000
    }
    DICT_FUTURE_CORE_CONTRACT = {
        'm': [1, 5, 9],
        'sr': [1, 5, 6],
        STR_50ETF: STR_ALL}

    DICT_TICK_SIZE = {
        "50etf": 0.0001,
        "m": 1,
        "sr": 0.5,
        'if': 0.2,
        'ih': 0.2,
        'ic': 0.2,
        'index': 0
    }

    DZQH_CF_DATA_MISSING_DATES = []

    @staticmethod
    def filter_invalid_data(x: pd.Series) -> bool:
        cur_date = x[Util.DT_DATE]
        if x[Util.DT_DATETIME] >= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 9, 30, 00) and \
                        x[
                            Util.DT_DATETIME] <= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 11, 30,
                                                                   00):
            return True
        if x[Util.DT_DATETIME] >= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 13, 00, 00) and \
                        x[
                            Util.DT_DATETIME] <= datetime.datetime(cur_date.year, cur_date.month, cur_date.day, 15, 00,
                                                                   00):
            return True
        return False

    @staticmethod
    def largest_element_less_than(list, val):
        for i in range(len(list), 0, -1):
            if list[i - 1] < val:
                return list[i - 1]
            elif i == 0:
                return None
            else:
                continue


class QuantlibUtil:
    @staticmethod
    def to_dt_dates(ql_dates):
        datetime_dates = []
        for d in ql_dates:
            dt = datetime.date(d.year(), d.month(), d.dayOfMonth())
            datetime_dates.append(dt)
        return datetime_dates

    @staticmethod
    def to_ql_dates(datetime_dates):
        ql_dates = []
        for d in datetime_dates:
            dt = ql.Date(d.day, d.month, d.year)
            ql_dates.append(dt)
        return ql_dates

    @staticmethod
    def to_ql_date(datetime_date):
        dt = ql.Date(datetime_date.day, datetime_date.month, datetime_date.year)
        return dt

    @staticmethod
    def to_dt_date(ql_date):
        dt = datetime.date(ql_date.year(), ql_date.month(), ql_date.dayOfMonth())
        return dt

    @staticmethod
    def get_yield_ts(evalDate, curve, mdate, daycounter):
        maxdate = curve.maxDate()
        if mdate > maxdate:
            rf = curve.zeroRate(maxdate, daycounter, ql.Continuous).rate()
        else:
            rf = curve.zeroRate(mdate, daycounter, ql.Continuous).rate()
        yield_ts = ql.YieldTermStructureHandle(ql.FlatForward(evalDate, rf, daycounter))
        return yield_ts

    @staticmethod
    def get_dividend_ts(evalDate, daycounter):
        dividend_ts = ql.YieldTermStructureHandle(ql.FlatForward(evalDate, 0.0, daycounter))
        return dividend_ts


class Statistics:
    @staticmethod
    def moving_average(df_series, n):
        ma = df_series.rolling(window=n).mean()
        return ma

    @staticmethod
    def standard_deviation(df_series, n):
        std = df_series.rolling(window=n).std()
        return std

    @staticmethod
    def percentile(df_series, n, percent):
        return df_series.rolling(window=n).quantile(percent)

    @staticmethod
    def volatility_by_closes(df_series_closes, n=20):
        series = np.log(df_series_closes).diff()
        vol = series.rolling(window=n).std() * math.sqrt(252)
        return vol
