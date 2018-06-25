import numpy as np
import scipy.stats as ss


def calculate_histvol(data, n):
    datas = np.log(data)
    df = datas.diff()
    vol = df.rolling(window=n).std() * np.sqrt(252)
    return vol


def simulate_minute(spot0, annualized_vol, T, mu=0.05, N=10000):
    dt = 1.0 / (365 * 1440.0)  # 1min
    T_minutes = T * 1440
    t = 1
    S = spot0
    path = [S]
    seeds = list(np.random.normal(0, 1, T_minutes))
    while t < T_minutes:
        normal = seeds[t - 1]
        ds = mu * S * dt + annualized_vol * S * normal * np.sqrt(dt)
        S += ds
        t += 1
        path.append(S)
    return S, path


def montecarlo(S0, sigma, T, mu=0.05, N=10000):
    pts = np.random.random((N, 2))


s, path = simulate_minute(3000, 0.2, 30)

print(s)
print(path)
