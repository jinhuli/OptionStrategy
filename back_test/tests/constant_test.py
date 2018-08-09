from back_test.model.constant import OptionUtil, OptionType

dict = {
    2.35:1,
    2.4:1 ,
    2.45: -1,
    2.5: 0,
    2.55: 1,
    2.6: 1,
    2.65: 2,
    2.7: 3,
    2.75: 4,
    2.8: 5,
    2.85: 6,
    2.9: 7,
    2.95: 8,
    3.0: 9,
    3.1: 10,
    3.2: 11,
    3.3: 12,
    3.4: 13
}
# print(OptionUtil.get_strike_monenyes_rank_dict_nearest_strike(2.78,dict.keys(),OptionType.CALL))
# print(OptionUtil.get_strike_by_monenyes_rank_nearest_strike(2.78, 1, dict.keys(), OptionType.CALL))
# print(OptionUtil.get_strike_by_monenyes_rank_nearest_strike(2.78, 0, dict.keys(), OptionType.CALL))
# print(OptionUtil.get_strike_by_monenyes_rank_nearest_strike(2.78, -1, dict.keys(), OptionType.CALL))
# print(OptionUtil.get_strike_monenyes_rank_dict_nearest_strike(2.78,dict.keys(),OptionType.PUT))
# print(OptionUtil.get_strike_by_monenyes_rank_nearest_strike(2.78, 1, dict.keys(), OptionType.PUT))
# print(OptionUtil.get_strike_by_monenyes_rank_nearest_strike(2.78, 0, dict.keys(), OptionType.PUT))
# print(OptionUtil.get_strike_by_monenyes_rank_nearest_strike(2.78, -1, dict.keys(), OptionType.PUT))
print("ATM moneyness")
print("2.78, call")
print(OptionUtil.get_strike_monenyes_rank_dict_nearest_strike(2.78,dict.keys(),OptionType.CALL))
print("2.78, put")
print(OptionUtil.get_strike_monenyes_rank_dict_nearest_strike(2.78,dict.keys(),OptionType.PUT))
print("2.76, call")
print(OptionUtil.get_strike_monenyes_rank_dict_nearest_strike(2.76,dict.keys(),OptionType.CALL))
print("2.76, put")
print(OptionUtil.get_strike_monenyes_rank_dict_nearest_strike(2.76,dict.keys(),OptionType.PUT))
print("OTM moneyness")
print("2.78, call")
print(OptionUtil.get_strike_monenyes_rank_dict_otm_strike(2.78,dict.keys(),OptionType.CALL))
print("2.78, put")
print(OptionUtil.get_strike_monenyes_rank_dict_otm_strike(2.78,dict.keys(),OptionType.PUT))
print("2.76, call")
print(OptionUtil.get_strike_monenyes_rank_dict_otm_strike(2.76,dict.keys(),OptionType.CALL))
print("2.76, put")
print(OptionUtil.get_strike_monenyes_rank_dict_otm_strike(2.76,dict.keys(),OptionType.PUT))
