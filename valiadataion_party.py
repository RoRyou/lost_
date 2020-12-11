import pandas as pd
df5 = pd.read_csv('D:/data/final_data_5.csv')
df6 = pd.read_csv('D:/data/final_data_6.csv')
df7 = pd.read_csv('D:/data/final_data_7.csv')
df8 = pd.read_csv('D:/data/final_data_8.csv')
df9 = pd.read_csv('D:/data/final_data_9.csv')
df10 = pd.read_csv('D:/data/final_data_10.csv')



print(df5[['LOST','isGX']])
aaa = df5[['LOST','isGX','isparty']]





print(len(aaa[aaa['isGX']==1]))
print(aaa[aaa['isGX']==1])

dict ={}
for key in aaa[aaa['isGX']==1]['LOST'].tolist():
    dict[key] = dict.get(key, 0) + 1

print(dict)


dict ={}
for key in aaa[aaa['isGX']==0]['LOST'].tolist():
    dict[key] = dict.get(key, 0) + 1

print(dict)


print(aaa[aaa['isparty']==1])

bbb= aaa[aaa['isparty']==1]

dict ={}
for key in bbb[bbb['isGX']==1]['LOST'].tolist():
    dict[key] = dict.get(key, 0) + 1
print(dict)


print(df5[(df5['isGX']==1)&(df5['isparty']==0)][['isGX','LOST','isparty']])

print('总人数')
print(len(aaa))
print('流失人数')
print(len(aaa[aaa['LOST']==1]))
print('参与帮会人数')
print(len(aaa[aaa['isparty']==1]))
print('参与帮会流失人数')
print(len(aaa[(aaa['isparty']==1)&(aaa['LOST']==1)]))
