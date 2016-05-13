import pandas as pd

df = pd.DataFrame([[0,1,2],[3,4,5]], columns=['a','b','c'])
df.ix[1, 'b'] =7
print df