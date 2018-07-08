from back_test.model.constant import OptionUtil

dict = {
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

print(OptionUtil.get_strike_by_monenyes_rank(2.76, 0, dict.keys(), -1))
print(OptionUtil.get_strike_by_monenyes_rank(2.78, -1, dict.keys(), -1))
print(OptionUtil.get_strike_by_monenyes_rank(2.78, -1, dict.keys(), 1))
print(OptionUtil.get_strike_by_monenyes_rank(3.16, 0, dict.keys()), -1)