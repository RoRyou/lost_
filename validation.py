import pandas as pd
df5 = pd.read_csv('D:/data/final_data_5.csv')
df6 = pd.read_csv('D:/data/final_data_6.csv')
df7 = pd.read_csv('D:/data/final_data_7.csv')
df8 = pd.read_csv('D:/data/final_data_8.csv')
df9 = pd.read_csv('D:/data/final_data_9.csv')
df10 = pd.read_csv('D:/data/final_data_10.csv')




dict = {}
for key in df10['LOST'].tolist():
    dict[key] = dict.get(key, 0) + 1
print('A5服务器总流失率')
print("%.2f%%" %(dict[1]/(dict[0]+dict[1])))


# print('------------')
# ACT_lists=['isparty', 'isXMSL','isLYQ','isKTT','isXMHJ','isSYZC','isPTY','isFBJL','isFBRH','dyLMZ', 'dyXMSL',  'dyLYQ', 'dyKTT',  'dyXMHJ',  'dySYZC', 'dyPTY',  'dyFBJL',  'dyFBRH',  'dybanggong','dyfee', 'isfee', 'dyForge_time', 'dyrate', 'dykilltimes','dykilledtimes']
# for ACT in ACT_lists:
#     dict = {}
#     for key in df5[df5[ACT] == 0]['LOST'].tolist():
#         dict[key] = dict.get(key, 0) + 1
#     if dict[1] == dict[0] + dict[1]:
#         print('所有人都参与了该活动')
#     else:
#         print('未玩过', ACT, '该活动流失率')
#         print("%.2f%%" %(dict[1]/(dict[0]+dict[1])))
#
#     dict = {}
#     for key in df5[df5[ACT] == 1]['LOST'].tolist():
#         dict[key] = dict.get(key, 0) + 1
#     if dict[1] == dict[0] + dict[1]:
#         print('所有人都参与了该活动')
#     else:
#         print('玩过',ACT,'该活动流失率')
#         print("%.2f%%" %(dict[1]/(dict[0]+dict[1])))
#         print('------------')

#
# print('------------')
# ACT_lists=['isparty', 'dyLMZ',  'dyXMSL', 'isXMSL', 'dyLYQ','isLYQ', 'dyKTT', 'isKTT', 'dyXMHJ', 'isXMHJ', 'dySYZC', 'isSYZC','dyPTY', 'isPTY', 'dyFBJL', 'isFBJL', 'dyFBRH', 'isFBRH', 'dybanggong','dyfee', 'isfee', 'dyForge_time', 'dyrate', 'dykilltimes','dykilledtimes']
# for ACT in ACT_lists:
#     dict = {}
#     for key in df6[df6[ACT] == 0]['LOST'].tolist():
#         dict[key] = dict.get(key, 0) + 1
#     if dict[1] == dict[0] + dict[1]:
#         print('所有人都参与了该活动')
#     else:
#         print('未玩过', ACT, '该活动流失率')
#         print(dict[1]/(dict[0]+dict[1]))
#
#     dict = {}
#     for key in df6[df6[ACT] == 1]['LOST'].tolist():
#         dict[key] = dict.get(key, 0) + 1
#     if dict[1] == dict[0] + dict[1]:
#         print('所有人都参与了该活动')
#     else:
#         print('玩过',ACT,'该活动流失率')
#         print(dict[1]/(dict[0]+dict[1]))
#         print('------------')
print('------------')
ACT_lists=['isparty', 'isXMSL','isLYQ', 'isKTT','isXMHJ','isSYZC','isPTY','isFBJL','isFBRH','dyLMZ', 'dyXMSL',  'dyLYQ', 'dyKTT',  'dyXMHJ',  'dySYZC', 'dyPTY',  'dyFBJL',  'dyFBRH',  'dybanggong','dyfee', 'isfee', 'dyForge_time', 'dyrate', 'dykilltimes','dykilledtimes']
for ACT in ACT_lists:
    dict = {}
    for key in df10[df10[ACT] == 0]['LOST'].tolist():
        dict[key] = dict.get(key, 0) + 1
    if dict[1] == dict[0] + dict[1]:
        print('所有人都参与了该活动')
    else:
        print('未玩过', ACT, '该活动流失率')
        print("%.2f%%" %(dict[1]/(dict[0]+dict[1])))

    dict = {}
    for key in df10[df10[ACT] == 1]['LOST'].tolist():
        dict[key] = dict.get(key, 0) + 1
    if dict[1] == dict[0] + dict[1]:
        print('所有人都参与了该活动')
    else:
        print('玩过',ACT,'该活动流失率')
        print("%.2f%%" %(dict[1]/(dict[0]+dict[1])))
        print('------------')