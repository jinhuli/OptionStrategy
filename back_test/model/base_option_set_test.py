import  datetime
from data_access.get_data import get_50option_mktdata
from back_test.model.base_option_set import BaseOptionSet
from back_test.model.constant import OptionType
start_date = datetime.date(2017, 11, 18)
end_date = datetime.date(2018, 1, 22)
df_option_metrics = get_50option_mktdata(start_date, end_date)
bkt_optionset = BaseOptionSet(df_option_metrics)
bkt_optionset.init()
while bkt_optionset.has_next():
    bkt_optionset.next()
    t_a = datetime.datetime.now()
    c = bkt_optionset.get_mdt_options_dict_by_moneyness_from_neares_value(2.73,0,OptionType.CALL)
    t_b = datetime.datetime.now()
    p = bkt_optionset.get_mdt_options_dict_by_moneyness_from_neares_value(2.73,0,OptionType.PUT)
    t_c = datetime.datetime.now()
    print("call used %{0} seconds, put used %{1} seconds".format((t_b - t_a).total_seconds(), (t_c - t_b).total_seconds()))
    # c,p = bkt_optionset.get_maturities_option_dict()