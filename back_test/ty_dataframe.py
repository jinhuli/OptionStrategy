import pandas as pd
import datetime
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from scipy.stats import norm

df = pd.DataFrame({
    'a': [1,2,3,4,5,6],
    'b': [1,2,3,4,5,6],
    'c': [1,2,3,4,5,6],
     'd': [7,6,4,8,5,6]
})
df1 = pd.DataFrame({
    'a': [1,2,3,4,5,],
    'b': [1,2,3,4,5,],
    'c': [1,2,3,4,5,],
     'd': [7,6,4,8,5,]
})


a = [1,2,3,4]
b = [3,4,5,6]
print([1]*len(a))
a_ = set(a)
b_ = set(b)
a_ = a_.intersection(b_)
print(a_)

for i,e in enumerate(a_):
    print(i,e)

print(a_)
t = set()
t.add('s')
t.add(1)
print(t)
t.remove(1)
print(t)

print(type(datetime.date(2000,1,1)))

name = 'sr_1707_c_6200'
print(name[-4:])
# df = df.sort_values('d')
# for (idx,row) in df.iterrows():
#     df.loc[idx,'add'] = row['d']-row['c']
# print(df)
# df = df.loc[:, df.columns != 'b'].join(df[['b']].rank(method='dense'))
# n = len(df)
# df['weight'] = df['b']-(n+1)/2
# print(df)
# c = sum(df[df['weight']>0]['weight'])
# df['weight'] = df['weight']/c
# print(df)
# print(sum(df[df['weight']>0]['weight']))
# print(df)
# print('-'*10)
#
# print(df['a'][0:2].tolist())

# print(df)
# print('-'*10)
# print(df[0:2])
# print('-'*10)
# df = df.reset_index()
# print('='*10)
# print(df.loc[0:2])
# print('-'*10)
# print(df[0:1])
# print('='*10)
# print(df.loc[len(df)-2:])
# print('-'*10)
# print(df[len(df)-2:])

# a = [1,2,3,4,5]
# i = [0]*len(a)

# print(a)
# print(i)
