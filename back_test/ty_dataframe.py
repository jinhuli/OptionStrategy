import pandas as pd
import datetime
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

df = pd.DataFrame({
    'a': [1,2,3,4,5,6],
    'b': [1,2,3,4,5,6],
    'c': [1,2,3,4,5,6],
     'd': [7,6,4,8,5,6]
})
df = df.sort_values('d')
for (idx,row) in df.iterrows():
    print(idx)
    df.loc[idx,'add'] = row['d']-row['c']

# print(df)
# print('-'*10)
# print(df[0:2])
# print('-'*10)
df = df.reset_index()
print('='*10)
print(df.loc[0:2])
print('-'*10)
print(df[0:2])
print('='*10)
print(df.loc[len(df)-2:])
print('-'*10)
print(df[len(df)-2:])

a = [1,2,3,4,5]
i = [0]*len(a)

print(a)
print(i)

