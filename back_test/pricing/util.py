import back_test.model.constant as constant


class Func(object):
    @staticmethod
    def payoff(spot: float, strike: float, option_type: constant.OptionType):
        return abs(max(option_type.value * (spot - strike), 0.0))
