import pandas as pd

a = pd.DataFrame([
    {
        'a': 1,
        'b': 2
    },
    {
        'a': 1.5,
        'b': 2.5
    },
    {
        'a': 1.5,
        'b': 2.5
    }
])

b = {
        'a': 1,
        'b': 2
    }

print(b.popitem()[1])