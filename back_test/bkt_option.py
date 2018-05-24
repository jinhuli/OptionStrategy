from OptionStrategyLib.OptionPricing.Options import OptionPlainEuropean

from OptionStrategyLib.OptionPricing.OptionMetrics import OptionMetrics
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation
from back_test.bkt_util import BktUtil

import datetime
import QuantLib as ql
import numpy as np
import pandas as pd
from back_test.bkt_instrument import BktInstrument

class BktOption(BktInstrument):
    """
    Contain metrics and trading position info as attributes

    """

    def __init__(self, cd_frequency, df_daily_metrics, flag_calculate_iv, df_intraday_metrics=None,
                 pricing_type='OptionPlainEuropean', engine_type='AnalyticEuropeanEngine',rf = 0.03):
        BktInstrument.__init__(self,cd_frequency,df_daily_metrics,rf=rf)
        self.util = BktUtil()
        self.flag_calculate_iv = flag_calculate_iv
        self.pricing_type = pricing_type
        self.engine_type = engine_type
        self.daycounter = ql.ActualActual()
        self.calendar = ql.China()
        self.start()

    "Add option specifics to start"
    def start(self):
        self.set_option_basics()
        self.update_pricing_metrics()

    def next(self):
        self.last_state = self.current_state
        self.current_index = self.current_index + 1
        self.implied_vol = None
        self.update_current_state()
        self.update_pricing_metrics()

    def update_evaluation(self):
        ql_evalDate = ql.Date(self.dt_date.day, self.dt_date.month, self.dt_date.year)
        evaluation = Evaluation(ql_evalDate, self.daycounter, self.calendar)
        self.evaluation = evaluation

    def set_option_basics(self):
        self.update_option_type()
        self.update_strike()
        self.update_maturitydt()
        self.update_multiplier()

    def update_pricing_metrics(self):
        self.update_evaluation()
        self.update_option_price()
        self.update_underlying()
        if self.pricing_metrics == None:
            if self.pricing_type == 'OptionPlainEuropean':
                ql_maturitydt = ql.Date(self.maturitydt.day,
                                        self.maturitydt.month,
                                        self.maturitydt.year)
                if self.option_type == 'call':
                    ql_optiontype = ql.Option.Call
                elif self.option_type == 'put':
                    ql_optiontype = ql.Option.Put
                else:
                    print('No option type!')
                    return
                option = OptionPlainEuropean(self.strike, ql_maturitydt, ql_optiontype)
            else:
                print('Unsupported Option Type !')
                option = None
            self.pricing_metrics = OptionMetrics(option, self.rf, self.engine_type).set_evaluation(self.evaluation)
        else:
            self.pricing_metrics.set_evaluation(self.evaluation)
        self.implied_vol = None

    def update_strike(self):
        try:
            strike = self.current_daily_state[self.util.col_strike]
        except Exception as e:
            print(e)
            strike = None
        try:
            adj_strike = self.current_daily_state[self.util.col_adj_strike]
        except Exception as e:
            print(e)
            adj_strike = None
        self.strike = strike
        self.adj_strike = adj_strike

    def update_maturitydt(self):
        try:
            maturitydt = self.current_daily_state[self.util.col_maturitydt]
        except Exception as e:
            print(e)
            print(self.current_daily_state)
            maturitydt = None
        self.maturitydt = maturitydt

    def update_option_type(self):
        try:
            option_type = self.current_daily_state[self.util.col_option_type]
        except Exception as e:
            print(e)
            option_type = None
        self.option_type = option_type

    def update_option_price(self):
        try:
            settle = self.current_state[self.util.col_settlement]
            close = self.current_state[self.util.col_close]
            if close != -999.0:
                option_price = close
            elif settle != -999.0:
                option_price = settle
            else:
                print(self.id_instrument, ' : amt_close and amt_settlement are null!', )
                option_price = None
        except Exception as e:
            print(e)
            option_price = None
        try:
            adj_option_price = self.current_state[self.util.col_adj_option_price]
        except Exception as e:
            print(e)
            adj_option_price = None
        try:
            amt_open = self.current_state[self.util.col_open]
        except Exception as e:
            print(e)
            amt_open = None

        self.option_price = option_price
        self.adj_option_price = adj_option_price
        self.option_price_open = amt_open

        try:
            self.option_morning_open_15min = self.current_state['amt_morning_open_15min']
        except:
            self.option_morning_open_15min = None
        try:
            self.option_morning_close_15min = self.current_state['amt_morning_close_15min']
        except:
            self.option_morning_close_15min = None
        try:
            self.option_afternoon_open_15min = self.current_state['amt_afternoon_open_15min']
        except:
            self.option_afternoon_open_15min = None
        try:
            self.option_afternoon_close_15min = self.current_state['amt_afternoon_close_15min']
        except:
            self.option_afternoon_close_15min = None
        try:
            self.option_morning_avg = self.current_state['amt_morning_avg']
        except:
            self.option_morning_avg = None
        try:
            self.option_afternoon_avg = self.current_state['amt_afternoon_avg']
        except:
            self.option_afternoon_avg = None
        try:
            self.option_daily_avg = self.current_state['amt_daily_avg']
        except:
            self.option_daily_avg = None

    def update_underlying(self):
        try:
            underlying_price = self.current_state[self.util.col_underlying_price]
            id_underlying = self.current_state[self.util.col_id_underlying]
        except Exception as e:
            print(e)
            underlying_price = None
            id_underlying = None

        try:
            if not self.last_state.empty:
                underlying_last_price = self.last_state[self.util.col_underlying_price]
            else:
                underlying_last_price = self.current_state[self.util.col_underlying_open_price]
        except Exception as e:
            print(e)
            underlying_last_price = None
        try:
            underlying_open_price = self.current_state[self.util.col_underlying_open_price]
        except Exception as e:
            print(e)
            underlying_open_price = underlying_last_price

        self.underlying_price = underlying_price
        self.id_underlying = id_underlying
        self.underlying_last_price = underlying_last_price
        self.underlying_open_price = underlying_open_price

    def update_implied_vol(self,spot=None,option_price=None):
        if spot == None:
            spot = self.underlying_price
        if option_price == None:
            option_price = self.option_price
        try:
            # self.update_underlying()
            # self.update_option_price()
            if self.flag_calculate_iv:
                implied_vol = self.pricing_metrics.implied_vol(spot,option_price)
            else:
                implied_vol = self.get_implied_vol_given() / 100.0
        except Exception as e:
            print(e)
            implied_vol = None
        self.implied_vol = implied_vol

    def update_multiplier(self):
        try:
            multiplier = self.current_daily_state[self.util.col_multiplier]
        except Exception as e:
            print(e)
            multiplier = None
        self.multiplier = multiplier

    def get_implied_vol_given(self):
        try:
            implied_vol = self.current_daily_state[self.util.col_implied_vol]
        except Exception as e:
            print(e)
            implied_vol = None
        return implied_vol

    def get_close(self):
        try:
            option_price = self.current_daily_state[self.util.col_close]
        except Exception as e:
            print(e)
            option_price = None
        return option_price

    def get_underlying_close(self):
        try:
            p = self.current_daily_state[self.util.col_underlying_price]
        except Exception as e:
            print(e)
            p = None
        return p

    def get_settlement(self):
        try:
            amt_settle = self.current_daily_state[self.util.col_settlement]
        except Exception as e:
            print(e)
            amt_settle = None
        return amt_settle

    def get_underlying_last_close(self):
        idx_date = self.dt_list.index(self.dt_date)
        if idx_date == 0:
            return self.current_daily_state[self.util.col_close]
        df_last_state = self.df_daily_metrics.loc[idx_date - 1]
        amt_pre_close = df_last_state[self.util.col_underlying_price]
        return amt_pre_close

    def get_implied_vol(self):
        if self.implied_vol == None: self.update_implied_vol()
        return self.implied_vol

    def get_delta(self,iv=None):
        try:
            if iv == None:
                if self.implied_vol == None: self.update_implied_vol()
                delta = self.pricing_metrics.delta(self.underlying_price, self.implied_vol)
            else:
                delta = self.pricing_metrics.delta(self.underlying_price,iv)
        except Exception as e:
            print(e)
            delta = None
        return delta

    def get_theta(self):
        try:
            if self.implied_vol == None: self.update_implied_vol()
            theta = self.pricing_metrics.theta(self.underlying_price, self.implied_vol)
        except Exception as e:
            print(e)
            theta = None
        return theta

    def get_vega(self):
        if self.implied_vol == None: self.update_implied_vol()
        try:
            vega = self.pricing_metrics.vega(self.underlying_price, self.implied_vol)
        except Exception as e:
            print(e)
            vega = None
        return vega

    def get_rho(self):
        if self.implied_vol == None: self.update_implied_vol()
        try:
            rho = self.pricing_metrics.rho(self.underlying_price, self.implied_vol)
        except Exception as e:
            print(e)
            rho = None
        return rho

    def get_gamma(self):
        if self.implied_vol == None: self.update_implied_vol()
        try:
            gamma = self.pricing_metrics.gamma(self.underlying_price, self.implied_vol)
        except Exception as e:
            print(e)
            gamma = None
        return gamma

    def get_vomma(self):
        if self.implied_vol == None: self.update_implied_vol()
        try:
            vomma = self.pricing_metrics.vomma( self.underlying_price,  self.implied_vol)
        except Exception as e:
            print(e)
            vomma = None
        return vomma

    def get_iv_roll_down(self, black_var_surface, dt):  # iv(tao-1)-iv(tao), tao:maturity
        if self.implied_vol == None: self.update_implied_vol()
        try:
            mdt = self.maturitydt
            evalDate = self.dt_date
            ttm = (mdt - evalDate).days / 365.0
            black_var_surface.enableExtrapolation()
            implied_vol_t1 = black_var_surface.blackVol(ttm - dt, self.strike)
            iv_roll_down = implied_vol_t1 - self.implied_vol
        except Exception as e:
            # print(e)
            iv_roll_down = 0.0
        return iv_roll_down

    def get_carry(self, bvs, hp):
        ttm = (self.maturitydt - self.dt_date).days
        # if ttm - hp <= 0: # 期限小于hp
        #     return None, None, None, None
        iv_roll_down = self.get_iv_roll_down(bvs, hp/365.0)
        if np.isnan(iv_roll_down): iv_roll_down = 0.0
        vega = self.get_vega()
        theta = self.get_theta()
        try:
            option_carry = (vega * iv_roll_down - theta * ttm) / self.option_price - self.rf
        except:
            option_carry = None
        return option_carry

    def get_init_margin(self):
        if self.trade_long_short == self.util.long: return 0.0
        # 认购期权义务仓开仓保证金＝[合约前结算价+Max（12%×合约标的前收盘价-认购期权虚值，
        #                           7%×合约标的前收盘价)]×合约单位
        # 认沽期权义务仓开仓保证金＝Min[合约前结算价 + Max（12 %×合约标的前收盘价 - 认沽期权虚值，
        #                               7 %×行权价格），行权价格] ×合约单位
        amt_last_settle = self.get_last_settlement()
        amt_underlying_last_close = self.get_underlying_last_close()
        if self.option_type == 'call':
            otm = max(0, self.strike - self.underlying_price)
            tmp = amt_last_settle + max(0.12 * amt_underlying_last_close - otm,
                                                 0.07 * amt_underlying_last_close)
            init_margin = tmp * self.multiplier
        else:
            otm = max(0, self.underlying_price - self.strike)
            tmp = min(amt_last_settle + max(0.12 * amt_underlying_last_close - otm,0.07 * self.strike),
                      self.strike)
            init_margin = tmp * self.multiplier
        return init_margin

    def get_maintain_margin(self):
        if self.trade_long_short == self.util.long: return 0.0
        if self.frequency in self.util.cd_frequency_low and self.trade_dt_open == self.dt_date:
            return self.get_init_margin()
        # 认购期权义务仓维持保证金＝[合约结算价 + Max（12 %×合约标的收盘价 - 认购期权虚值，
        #                                           7 %×合约标的收盘价）]×合约单位
        # 认沽期权义务仓维持保证金＝Min[合约结算价 + Max（12 %×合标的收盘价 - 认沽期权虚值，7 %×行权价格），
        #                               行权价格]×合约单位
        amt_settle = self.get_settlement()
        amt_underlying_close = self.get_underlying_close()
        if self.option_type == 'call':
            otm = max(0, self.strike - self.underlying_price)
            maintain_margin = (amt_settle + max(0.12 * amt_underlying_close - otm,
                                                0.07 * amt_underlying_close)) * self.multiplier

        else:
            otm = max(0, self.underlying_price - self.strike)
            maintain_margin = min(amt_settle + max(0.12 * amt_underlying_close - otm,
                                                   0.07 * self.strike), self.strike) * self.multiplier
        return maintain_margin

    def price_limit(self):
        # 认购期权最大涨幅＝max｛合约标的前收盘价×0.5 %，min[（2×合约标的前收盘价－行权价格），合约标的前收盘价]×10％｝
        # 认购期权最大跌幅＝合约标的前收盘价×10％
        # 认沽期权最大涨幅＝max｛行权价格×0.5 %，min[（2×行权价格－合约标的前收盘价），合约标的前收盘价]×10％｝
        # 认沽期权最大跌幅＝合约标的前收盘价×10％
        return None

    def get_unit_by_mtmv(self, mtm_value):
        unit = np.floor(mtm_value / (self.option_price * self.multiplier))
        # unit = mtm_value/(self.option_price*self.multiplier)
        return unit


    def senario_calculate_option_price(self,underlying_price,vol):
        try:
            self.update_underlying()
            self.update_option_price()
            p = self.pricing_metrics.option_price(self.evaluation, self.rf,
                                                    underlying_price, vol, self.engine_type)
        except Exception as e:
            p = None
        return p