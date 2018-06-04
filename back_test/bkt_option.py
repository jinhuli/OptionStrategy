from OptionStrategyLib.OptionPricing.Options import OptionPlainEuropean
from OptionStrategyLib.OptionPricing.OptionMetrics import OptionMetrics
from OptionStrategyLib.OptionPricing.Evaluation import Evaluation
from back_test.bkt_util import BktUtil
import QuantLib as ql
import numpy as np
from back_test.bkt_instrument import BktInstrument
import datetime

class BktOption(BktInstrument):

    """ Contain metrics and trading position info as attributes """

    def __init__(self, cd_frequency, df_daily_metrics, flag_calculate_iv,
                 pricing_type='OptionPlainEuropean', engine_type='AnalyticEuropeanEngine', rf = 0.03):
        BktInstrument.__init__(self,cd_frequency,df_daily_metrics,rf=rf)
        self.util = BktUtil()
        self.flag_calculate_iv = flag_calculate_iv
        self.pricing_type = pricing_type
        self.engine_type = engine_type
        self.daycounter = ql.ActualActual()
        self.calendar = ql.China()
        self.implied_vol = None
        self.update_pricing_metrics()

    def next(self):
        self.current_index = self.current_index + 1
        self.implied_vol = None
        self.update_current_state()
        self.update_pricing_metrics()

    def update_evaluation(self):
        ql_evalDate = ql.Date(self.eval_date.day, self.eval_date.month, self.eval_date.year)
        evaluation = Evaluation(ql_evalDate, self.daycounter, self.calendar)
        self.evaluation = evaluation

    def update_pricing_metrics(self):
        self.update_evaluation()
        if self.pricing_metrics == None:
            if self.pricing_type == 'OptionPlainEuropean':
                ql_maturitydt = ql.Date(self.maturitydt().day,
                                        self.maturitydt().month,
                                        self.maturitydt().year)
                if self.option_type() == self.util.type_call:
                    ql_optiontype = ql.Option.Call
                elif self.option_type() == self.util.type_put:
                    ql_optiontype = ql.Option.Put
                else:
                    print('No option type!')
                    return
                option = OptionPlainEuropean(self.strike(), ql_maturitydt, ql_optiontype)
            else:
                print('Unsupported Option Type !')
                option = None
            self.pricing_metrics = OptionMetrics(option, self.rf, self.engine_type).set_evaluation(self.evaluation)
        else:
            self.pricing_metrics.set_evaluation(self.evaluation)

    def strike(self):
        try:
            strike = self.current_daily_state[self.util.col_strike]
        except Exception as e:
            print(e)
            strike = None
        return strike

    def adj_strike(self):
        try:
            adj_strike = self.current_daily_state[self.util.col_adj_strike]
        except Exception as e:
            print(e)
            adj_strike = None
        return adj_strike

    def maturitydt(self):
        try:
            maturitydt = self.current_daily_state[self.util.col_maturitydt]
        except:
            maturitydt = None
        return maturitydt

    def option_type(self):
        try:
            option_type = self.current_daily_state[self.util.col_option_type]
        except Exception as e:
            print(e)
            option_type = None
        return option_type

    """ 如果close price为空，使用settlement price作为option price """
    def option_price(self):
        try:
            settle = self.current_state[self.util.col_settlement]
            close = self.current_state[self.util.col_close]
            if close != self.util.nan_value:
                option_price = close
            elif settle != self.util.nan_value:
                option_price = settle
            else:
                print(self.id_instrument, ' : amt_close and amt_settlement are null!', )
                option_price = None
        except Exception as e:
            print('Option close price and settlement price are both nan. ', e)
            option_price = None
        return option_price

    def adj_option_price(self):
        try:
            adj_option_price = self.current_state[self.util.col_adj_option_price]
            if adj_option_price < 0: return None
        except:
            adj_option_price = None
        return adj_option_price

    def id_underlying(self):
        try:
            id_underlying = self.current_state[self.util.col_id_underlying]
        except:
            id_underlying = None
        return id_underlying

    def underlying_close(self):
        try:
            underlying_price = self.current_state[self.util.col_underlying_close]
            if underlying_price == self.util.nan_value: return None
        except:
            underlying_price = None
        return underlying_price

    """ last bar/state, not necessarily daily"""
    def underlying_last_close(self):
        try:
            if self.current_index > 0:
                last_state = self.df_metrics.loc[self.current_index-1]
                underlying_last_price = last_state[self.util.col_underlying_close]
                if underlying_last_price == self.util.nan_value : return None
            else:
                """ if no previous date, use OPEN price """
                underlying_last_price = self.current_state[self.util.col_underlying_open_price]
        except Exception as e:
            print(e)
            underlying_last_price = None
        return underlying_last_price

    def underlying_open_price(self):
        try:
            underlying_open_price = self.current_state[self.util.col_underlying_open_price]
            if underlying_open_price == self.util.nan_value: return None
        except:
            underlying_open_price = None
        return underlying_open_price

    def update_implied_vol(self,spot=None,option_price=None):
        if spot == None:
            spot = self.underlying_close()
        if option_price == None:
            option_price = self.option_price()
        try:
            if self.flag_calculate_iv:
                implied_vol = self.pricing_metrics.implied_vol(spot,option_price)
            else:
                implied_vol = self.implied_vol_given() / 100.0
        except Exception as e:
            print(e)
            implied_vol = None
        self.implied_vol = implied_vol

    def multiplier(self):
        try:
            multiplier = self.current_daily_state[self.util.col_multiplier]
        except:
            multiplier = None
        return multiplier

    def implied_vol_given(self):
        try:
            implied_vol = self.current_daily_state[self.util.col_implied_vol]
        except Exception as e:
            print(e)
            implied_vol = None
        return implied_vol

    def get_implied_vol(self):
        if self.implied_vol == None: self.update_implied_vol()
        return self.implied_vol

    def get_delta(self,iv=None):
        try:
            if iv == None:
                if self.implied_vol == None: self.update_implied_vol()
                delta = self.pricing_metrics.delta(self.underlying_close(), self.implied_vol)
            else:
                delta = self.pricing_metrics.delta(self.underlying_close(),iv)
        except Exception as e:
            print(e)
            delta = None
        return delta

    def get_theta(self):
        try:
            if self.implied_vol == None: self.update_implied_vol()
            theta = self.pricing_metrics.theta(self.underlying_close(), self.implied_vol)
        except Exception as e:
            print(e)
            theta = None
        return theta

    def get_vega(self):
        if self.implied_vol == None: self.update_implied_vol()
        try:
            vega = self.pricing_metrics.vega(self.underlying_close(), self.implied_vol)
        except Exception as e:
            print(e)
            vega = None
        return vega

    def get_rho(self):
        if self.implied_vol == None: self.update_implied_vol()
        try:
            rho = self.pricing_metrics.rho(self.underlying_close(), self.implied_vol)
        except Exception as e:
            print(e)
            rho = None
        return rho

    def get_gamma(self):
        if self.implied_vol == None: self.update_implied_vol()
        try:
            gamma = self.pricing_metrics.gamma(self.underlying_close(), self.implied_vol)
        except Exception as e:
            print(e)
            gamma = None
        return gamma

    def get_vomma(self):
        if self.implied_vol == None: self.update_implied_vol()
        try:
            vomma = self.pricing_metrics.vomma( self.underlying_close(),  self.implied_vol)
        except Exception as e:
            print(e)
            vomma = None
        return vomma

    def get_iv_roll_down(self, black_var_surface, dt):  # iv(tao-1)-iv(tao), tao:maturity
        if self.implied_vol == None: self.update_implied_vol()
        try:
            mdt = self.maturitydt()
            evalDate = self.eval_date
            ttm = (mdt - evalDate).days / 365.0
            black_var_surface.enableExtrapolation()
            implied_vol_t1 = black_var_surface.blackVol(ttm - dt, self.strike())
            iv_roll_down = implied_vol_t1 - self.implied_vol
        except Exception as e:
            print(e)
            iv_roll_down = 0.0
        return iv_roll_down

    def get_carry(self, bvs, hp):
        ttm = (self.maturitydt() - self.eval_date).days
        # if ttm - hp <= 0: # 期限小于hp
        #     return None, None, None, None
        iv_roll_down = self.get_iv_roll_down(bvs, hp/365.0)
        if np.isnan(iv_roll_down): iv_roll_down = 0.0
        vega = self.get_vega()
        theta = self.get_theta()
        try:
            option_carry = (vega * iv_roll_down - theta * ttm) / self.option_price() - self.rf
        except:
            option_carry = None
        return option_carry

    def get_init_margin(self):
        if self.trade_long_short == self.util.long: return 0.0
        # 认购期权义务仓开仓保证金＝[合约前结算价+Max（12%×合约标的前收盘价-认购期权虚值，
        #                           7%×合约标的前收盘价)]×合约单位
        # 认沽期权义务仓开仓保证金＝Min[合约前结算价 + Max（12 %×合约标的前收盘价 - 认沽期权虚值，
        #                               7 %×行权价格），行权价格] ×合约单位
        amt_last_settle = self.mktprice_last_settlement()
        amt_underlying_last_close = self.underlying_last_close()
        if self.option_type() == self.util.type_call:
            otm = max(0, self.strike() - self.underlying_close())
            tmp = amt_last_settle + max(0.12 * amt_underlying_last_close - otm,
                                                 0.07 * amt_underlying_last_close)
            init_margin = tmp * self.multiplier()
        else:
            otm = max(0, self.underlying_close() - self.strike())
            tmp = min(amt_last_settle + max(0.12 * amt_underlying_last_close - otm,0.07 * self.strike()),
                      self.strike())
            init_margin = tmp * self.multiplier()
        return init_margin

    def get_maintain_margin(self):
        if self.trade_long_short == self.util.long: return 0.0
        if self.frequency in self.util.cd_frequency_low and self.trade_dt_open == self.eval_date:
            return self.get_init_margin()
        # 认购期权义务仓维持保证金＝[合约结算价 + Max（12 %×合约标的收盘价 - 认购期权虚值，
        #                                           7 %×合约标的收盘价）]×合约单位
        # 认沽期权义务仓维持保证金＝Min[合约结算价 + Max（12 %×合标的收盘价 - 认沽期权虚值，7 %×行权价格），
        #                               行权价格]×合约单位
        amt_settle = self.mktprice_settlement()
        if amt_settle==None or amt_settle==np.nan:
            amt_settle = self.mktprice_close()
        amt_underlying_close = self.underlying_close()
        # if self.eval_date == datetime.date(2018,1,26):
        #     print(self.eval_date)
        print(self.eval_date,self.id_instrument(), amt_underlying_close, amt_settle)
        if self.option_type() == self.util.type_call:
            otm = max(0, self.strike() - amt_underlying_close)
            maintain_margin = (amt_settle + max(0.12 * amt_underlying_close - otm,
                                                0.07 * amt_underlying_close)) * self.multiplier()

        else:
            otm = max(0, amt_underlying_close - self.strike())
            maintain_margin = min(amt_settle +
                                  max(0.12 * amt_underlying_close - otm, 0.07 * self.strike()),
                                  self.strike()) * self.multiplier()
        return maintain_margin

    def price_limit(self):
        # 认购期权最大涨幅＝max｛合约标的前收盘价×0.5 %，min[（2×合约标的前收盘价－行权价格），合约标的前收盘价]×10％｝
        # 认购期权最大跌幅＝合约标的前收盘价×10％
        # 认沽期权最大涨幅＝max｛行权价格×0.5 %，min[（2×行权价格－合约标的前收盘价），合约标的前收盘价]×10％｝
        # 认沽期权最大跌幅＝合约标的前收盘价×10％
        return None

    def get_unit_by_mtmv(self, mtm_value):
        unit = np.floor(mtm_value / (self.option_price() * self.multiplier()))
        # unit = mtm_value/(self.option_price*self.multiplier)
        return unit

    def senario_calculate_option_price(self,underlying_price,vol):
        try:
            p = self.pricing_metrics.option_price(self.evaluation, self.rf, underlying_price,
                                                  vol, self.engine_type)
        except:
            p = None
        return p