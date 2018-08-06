import typing
import math
import datetime
import back_test.model.constant as constant


class OptionPayoff(object):
    @staticmethod
    def get_payoff(s: float, k: float):
        return max(s - k, 0)


"""
                 dt_eval: datetime.date,
                 dt_maturity: datetime.date,
                 strike: float,
                 type: OptionType,
                 spot: float,
                 vol: float,
                 rf: float = 0.03
"""


class BinomialTree(object):

    def __init__(self, n: int, dt_eval: datetime.date, dt_maturity: datetime.date,
                 option_type: constant.OptionType, option_exercise_type: constant.OptionExerciseType,
                 spot: float, strike: float, vol: float, rf: float = 0.03):

        self.values: typing.List[typing.List[float]] = []
        self.asset_values: typing.List[typing.List[float]] = []
        self.exercise_values: typing.List[typing.List[float]] = []
        self.option_type = option_type
        self.option_exercise_type = option_exercise_type
        self.strike = strike
        self.spot = spot
        self.dt_eval: datetime.date = dt_eval
        self.dt_maturity: datetime.date = dt_maturity
        self.t: float = (dt_maturity - dt_eval).days / n / 365.0
        self.u = math.exp(vol * math.sqrt(self.t))
        self.d = math.exp(-1 * vol * math.sqrt(self.t))
        self.p_u = (math.exp(rf * self.t) - self.d) / (self.u - self.d)
        self.p_d = 1 - self.p_u
        self.discount = math.exp(-1 * rf * self.t)
        self.n: int = n

    def initialize(self):
        self.populate_asset()

    def populate_asset(self):
        pre = []
        for i in range(self.n):
            if len(pre) == 0:
                cur = [self.spot]
            else:
                cur = [pre[0] * self.u]
                for item in pre:
                    cur.append(item * self.d)
            self.asset_values.append(cur)
            pre = cur
        for asset_value in self.asset_values:
            exercise_value = [constant.PricingUtil.payoff(v, self.strike, self.option_type) for v in asset_value]
            self.exercise_values.append(exercise_value)

    def disp(self):
        print("exercise type =", self.option_exercise_type)
        print("u =", self.u)
        print("d =", self.d)
        print("p_u =", self.p_u)
        print("p_d =", self.p_d)
        # if len(self.asset_values) != 0:
        #     print("asset(underlying) tree structure")
        #     for value in self.asset_values:
        #         print(value)
        # if len(self.exercise_values) != 0:
        #     print("exercise tree structure")
        #     for value in self.exercise_values:
        #         print(value)
        # if len(self.values) != 0:
        #     print("value tree structure")
        #     for value in self.values:
        #         print(value)

    def size(self, i: int) -> int:
        return i

    def NPV(self) -> float:
        return self.values[0][0]

    def step_back(self, step_back_to: int) -> None:
        step_back_from = self.n - 1
        self.values.insert(0, self.exercise_values[step_back_from])
        for i in range(step_back_from, step_back_to, -1):
            cur_value = self.values[0]
            pre_value = []
            count = self.size(i)
            for j in range(count):
                continous_value = (cur_value[j] * self.p_u + cur_value[j + 1] * self.p_d) * self.discount
                if self.option_exercise_type == constant.OptionExerciseType.AMERICAN:
                    exercise_value = self.exercise_values[i - 1][j]
                else:
                    exercise_value = 0
                pre_value.append(max(continous_value, exercise_value))
            self.values.insert(0, pre_value)
        return None
