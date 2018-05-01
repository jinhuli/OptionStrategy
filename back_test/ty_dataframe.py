import pandas as pd

import datetime
df1 = pd.DataFrame({
    'a': [0,2,3,4,5,],
    'b': [1,2,3,4,5,],
    'c': [1,2,3,4,5,],
     'd': [7,6,4,8,5,]
})
print(df1.columns.values)
print(len(df1[df1.columns[0]]))
print(len(df1))
for (idx,r) in df1.iterrows():
    r['a'] = 100
print(df1)
# df1= df1.sort_values(by=['d'],ascending=True)
# print(df1)
# print(df1.at[1,'a'])
# print(df1.iat[1,1])
#
# d = datetime.date(2018,1,1)-datetime.date(1999,1,1)
# print(d.days)
#
# a = [{'a':1},{'a':2}]
# a.remove({'a':1})
# print(a)
#
# b = {'b':2}
# b.update({'c':1})
# print(b)

# a = [1,2,3,4,5]
# print(a.index(2))
# print(a[a.index(2):])