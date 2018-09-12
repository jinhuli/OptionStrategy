from back_test.model.base_option_set import BaseOptionSet
from data_access import get_data
import back_test.model.constant as c
import datetime
import pandas as pd

""""""
start_date = datetime.date(2015, 1, 4)
end_date = datetime.date.today()
min_holding = 1
nbr_maturitys = [0,1,2,3]

df_metrics = get_data.get_50option_mktdata(start_date, end_date)

list_capacity = []

optionset = BaseOptionSet(df_metrics)
optionset.init()
# optionset.go_to(end_date)
# 计算市场容量时直接用nbr_maturity选择相应的到期日，不需考虑min_holding
while optionset.has_next():
    for nbr_maturity in nbr_maturitys:
        dt_maturity = optionset.get_maturities_list()[nbr_maturity]
        dict_m_options = optionset.get_dict_moneyness_and_options(dt_maturity,c.OptionType.PUT)
        otm_put = 0 #[-inf, -1]
        atm_put = 0 #[0]
        itm_put = 0 #[1, inf]
        for m in dict_m_options.keys():
            if m <= -1:
                for option in dict_m_options[m]:
                    otm_put += option.trading_volume()
            elif m >= 1:
                for option in dict_m_options[m]:
                    itm_put += option.trading_volume()
            else:
                for option in dict_m_options[m]:
                    atm_put += option.trading_volume()
        total_put = otm_put+itm_put+atm_put
        total_call = 0
        dict_m_options_c = optionset.get_dict_moneyness_and_options(dt_maturity,c.OptionType.CALL)
        for m in dict_m_options_c.keys():
            for option in dict_m_options_c[m]:
                total_call += option.trading_volume()
        list_capacity.append({
            '1-dt_date':optionset.eval_date,
            '2-nbr_maturity':nbr_maturity,
            '3-otm_put':otm_put,
            '4-itm_put':itm_put,
            '5-atm_put':atm_put,
            '6-total_put':total_put,
            '7-total_option':total_call+total_put,
        })
        # print(optionset.eval_date,nbr_maturity,total_put,total_call)
    optionset.next()

df_capacity = pd.DataFrame(list_capacity)

df_m0 = df_capacity[df_capacity['2-nbr_maturity']==0].reset_index(drop=True)
df_m1 = df_capacity[df_capacity['2-nbr_maturity']==1].reset_index(drop=True)
df_m2 = df_capacity[df_capacity['2-nbr_maturity']==2].reset_index(drop=True)
df_m3 = df_capacity[df_capacity['2-nbr_maturity']==3].reset_index(drop=True)

df_m0['8-otm_put_capacity'] = c.Statistics.moving_average(df_m0['3-otm_put'],20)/100.0
df_m0['9-put_capacity'] = c.Statistics.moving_average(df_m0['6-total_put'],20)/100.0
df_m0['10-total_capacity'] = c.Statistics.moving_average(df_m0['7-total_option'],20)/100.0
df_m0.to_csv('../accounts_data/df_m0.csv')

df_m1['8-otm_put_capacity'] = c.Statistics.moving_average(df_m1['3-otm_put'],20)/100.0
df_m1['9-put_capacity'] = c.Statistics.moving_average(df_m1['6-total_put'],20)/100.0
df_m1['10-total_capacity'] = c.Statistics.moving_average(df_m1['7-total_option'],20)/100.0
df_m1.to_csv('../accounts_data/df_m1.csv')

df_m2['8-otm_put_capacity'] = c.Statistics.moving_average(df_m2['3-otm_put'],20)/100.0
df_m2['9-put_capacity'] = c.Statistics.moving_average(df_m2['6-total_put'],20)/100.0
df_m2['10-total_capacity'] = c.Statistics.moving_average(df_m2['7-total_option'],20)/100.0
df_m2.to_csv('../accounts_data/df_m2.csv')

df_m3['8-otm_put_capacity'] = c.Statistics.moving_average(df_m3['3-otm_put'],20)/100.0
df_m3['9-put_capacity'] = c.Statistics.moving_average(df_m3['6-total_put'],20)/100.0
df_m3['10-total_capacity'] = c.Statistics.moving_average(df_m3['7-total_option'],20)/100.0
df_m3.to_csv('../accounts_data/df_m3.csv')