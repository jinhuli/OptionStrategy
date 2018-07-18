import pandas as pd

a = pd.DataFrame([
    {
        'a': 1,
        'b': 2
    },
    {
        'a': 1.5,
        'b': 2.5
    }
])
print(a)

s = a.loc[1]

s['a'] = 2
s['b'] = 3
print('-------')
print(a)
