import pandas as pd

a = pd.DataFrame(data=[{'a':1,'b':3},{'a':2,'b':4}])
print(a)
if 'c' in a.columns:
    print("t")

a.reset_index(drop=True)
print(a)