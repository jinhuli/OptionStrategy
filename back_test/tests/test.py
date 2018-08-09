import back_test.model.constant as c
from back_test.model.constant import OptionM

d = [2400.0, 2450.0, 2500.0, 2550.0, 2600.0, 2650.0, 2700.0, 2750.0, 2800.0, 2850.0, 2900.0, 2950.0, 3000.0, 3050.0,
     3100.0, 3150.0]
spot = 2010
res = OptionM.get_strike_monenyes_rank_dict_nearest_strike(spot, d, c.OptionType.CALL)
print(spot)
print(d)
print(res)
spot = 3300
res = OptionM.get_strike_monenyes_rank_dict_nearest_strike(spot, d, c.OptionType.CALL)
print(spot)
print(d)
print(res)
spot = 2010
res = OptionM.get_strike_monenyes_rank_dict_nearest_strike(spot, d, c.OptionType.PUT)
print(spot)
print(d)
print(res)
spot = 3300
res = OptionM.get_strike_monenyes_rank_dict_nearest_strike(spot, d, c.OptionType.PUT)
print(spot)
print(d)
print(res)
