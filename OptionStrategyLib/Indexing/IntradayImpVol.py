from data_access.get_data import get_50option_mktdata as option_data, get_50option_intraday as intraday_data
from back_test.model.base_option_set import BaseOptionSet
from back_test.model.constant import Util, OptionUtil
import datetime
import math
import pandas as pd


class IntradayImpVol(BaseOptionSet):
    def __init__(self, start_date, end_date, min_holding):
        df_daily = option_data(start_date, end_date)
        df_intraday = intraday_data(start_date, end_date)
        super().__init__(df_data=df_intraday,df_daily_data=df_daily, rf=0.03)
        self.min_holding = min_holding


    def get_atm_options(self, nbr_maturity):
        maturity = self.select_maturity_date(nbr_maturity, min_holding=self.min_holding)
        list_atm_call, list_atm_put = self.get_options_list_by_moneyness_mthd1(moneyness_rank=0, maturity=maturity)
        atm_call = self.select_higher_volume(list_atm_call)
        atm_put = self.select_higher_volume(list_atm_put)
        return atm_call, atm_put

    def get_atm_iv_average(self, nbr_maturity):
        atm_call, atm_put = self.get_atm_options(nbr_maturity)
        iv_call = atm_call.get_implied_vol()
        iv_put = atm_put.get_implied_vol()
        iv_avg = (iv_call + iv_put) / 2
        return iv_avg


start_date = datetime.date.today() - datetime.timedelta(days=10)
end_date = datetime.date.today()
nbr_maturity = 0
min_holding = 8
Impvol = IntradayImpVol(start_date, end_date, min_holding)
Impvol.init()

# maturity = Impvol.select_maturity_date(nbr_maturity, min_holding=Impvol.min_holding)
# iv_htr = Impvol.get_atm_iv_by_htbr(maturity)
# iv_avg = Impvol.get_atm_iv_average(nbr_maturity)
# print('iv_htr : ', iv_htr)
# print('iv_avg : ', iv_avg)
#
#
# atm_call, atm_put = Impvol.get_atm_options(nbr_maturity) # 基于收盘价的平值期权
# id_atm_call = atm_call.id_instrument()

maturity = Impvol.select_maturity_date(nbr_maturity=nbr_maturity,min_holding=min_holding)
iv = Impvol.get_atm_iv_by_htbr(maturity)









