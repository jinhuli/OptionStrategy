import numpy as np
import scipy.stats as ss
import datetime
import pandas as pd



def calculate_histvol(data, n):
    datas = np.log(data)
    df = datas.diff()
    vol = df.rolling(window=n).std() * np.sqrt(252)
    return vol


def simulate(S0, sigma, N, mu=0.05):
    dt = 1.0 / (365 * 1440.0)  # 1min
    t = 1
    S = S0
    path = [S]
    seeds = list(np.random.normal(0, 1, N))
    while t < N:
        normal = seeds[t - 1]
        ds = mu * S * dt + sigma * S * normal * np.sqrt(dt)
        S += ds
        t += 1
        path.append(S)
    return S, path


def montecarlo(S0, sigma, dates, mu=0.05, N=10000):
    datetimes = generate_times(dates)
    N = len(datetimes)
    S, path = simulate(S0, sigma, N)
    df = pd.DataFrame()
    df['dt_datetime'] = datetimes
    df['amt_close'] = path
    df['dt_date'] = df['dt_datetime'].apply(lambda x: x.date())
    return df


def generate_times(dt_dates):
    dt_times = []
    for dt_date in dt_dates:
        dt_time = datetime.datetime(dt_date.year, dt_date.month, dt_date.day, 9, 30, 0)
        dt_times.append(dt_time)
        while dt_time < datetime.datetime(dt_date.year, dt_date.month, dt_date.day, 11, 30, 0):
            dt_time = dt_time + datetime.timedelta(minutes=1)
            dt_times.append(dt_time)
        dt_time = datetime.datetime(dt_date.year, dt_date.month, dt_date.day, 13, 0, 0)
        dt_times.append(dt_time)
        while dt_time < datetime.datetime(dt_date.year, dt_date.month, dt_date.day, 15, 0, 0):
            dt_time = dt_time + datetime.timedelta(minutes=1)
            dt_times.append(dt_time)
    return dt_times


# t = generate_times([datetime.date(2018, 1, 1), datetime.date(2018, 1, 2)])
# print(t)
montecarlo(3000, 0.2,[datetime.date(2018, 1, 1), datetime.date(2018, 1, 2)])
# s, path = simulate_minute(3000, 0.2, 30)
# print(s)
# print(path)
