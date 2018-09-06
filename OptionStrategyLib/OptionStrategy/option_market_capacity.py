from back_test.model.base_option_set import BaseOptionSet
from data_access import get_data
import back_test.model.constant as c
import datetime

""""""
end_date = datetime.date(2018, 9, 4)
last_week = datetime.date(2018, 8, 31)
start_date = last_week
min_holding = 1
nbr_maturity = 0

df_metrics = get_data.get_50option_mktdata(start_date, end_date)

optionset = BaseOptionSet(df_metrics)
optionset.init()
optionset.go_to(end_date)
dt_maturity = optionset.get_maturities_list()[nbr_maturity]

# 计算市场容量时直接用nbr_maturity选择相应的到期日，不需考虑min_holding
# df_current = optionset.get_current_state()
# df_mdt = df_current[df_current[c.Util.DT_MATURITY] == dt_maturity].reset_index(drop=True)
# df_put = df_mdt[df_mdt[c.Util.CD_OPTION_TYPE] == c.Util.STR_PUT]
# total_volume_put = df_put[c.Util.AMT_TRADING_VOLUME].sum()
# print('total_volume_put : ', total_volume_put)

mdt_calls, mdt_puts = optionset.get_orgnized_option_dict_for_moneyness_ranking()
mdt_options_dict = mdt_calls.get(dt_maturity)
spot = list(mdt_options_dict.values())[0][0].underlying_close()
strikes = list(mdt_options_dict.keys())
res = c.Option50ETF.get_strike_monenyes_rank_dict_nearest_strike(spot, strikes, c.OptionType.PUT)
print(res)
dict_res = {}
