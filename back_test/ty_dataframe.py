import pandas as pd
import datetime
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

df = pd.DataFrame({
    'a': [1,2,3,4,5,6],
    'c': [1,2,3,4,5,6],
    'd': [1,2,3,4,5,6],
     'b': [2,2,4,4,5,6]
})

print(df)
df1 = df.loc[len(df)-5:]

# df = df.drop_duplicates(subset=['b'])
# df = df.reset_index()
# print(df)
#
# print(df.columns.tolist())
#
#
# l = ['a','b','c']
# print(l)
# l.remove('c')
# print(l)

df2 = pd.DataFrame(columns=['bktoption'])
print(df.loc[len(df)-1,'a'])

list1 = [1,2,34,66,3,45,]
print(max(list1))

print("%20s %20s" %('annulized_return','max_drawdown'))