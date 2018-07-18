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
],index=["c","d"])
print(a)

s = a.loc['c']

s['a'] = 2
s['b'] = 3
# print(s)
# a.loc['c'] = s
print('-------')
print(a)
