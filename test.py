import pandas as pd
pd.set_option('display.width', 1000)

f = open('tst.csv', 'r')
df = pd.DataFrame([l.split(',') for l in f.readline().split(' ')])
print df.to_csv('temp.csv')
# print df
