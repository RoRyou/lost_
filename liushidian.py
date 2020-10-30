#coding: utf-8



import pandas as pd
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
import plotly.graph_objs as go
from sklearn.model_selection import train_test_split
from sklearn import tree
import numpy as np
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 这两行需要手动设置
from sklearn.metrics import confusion_matrix, accuracy_score, classification_report
from sklearn.ensemble import RandomForestRegressor

# pd.set_option('display.max_rows', None)  # 可以填数字，填None表示'行'无限制
# pd.set_option('display.max_columns', None) # 可以填数字，填None表示'列'无限制
# pd.set_option('display.width', 1000) #横向不换行

from sklearn import metrics

DATASPLITRATE = 0.2
MAXDEPTH =4
#MINIMPURITYDECRESASE=val
MINSAMPLESSPLIT =1


df1 = pd.read_csv('D:/data/A7更新次留所有人(7).csv')
# df3 = pd.read_csv('D:/data/A7_final_13.csv')
#
# df1=df3
print(df1.columns)


print('仙门试炼平均值',df1['仙门试炼次数'].mean(axis=0))






df1.drop(df1.loc[df1['0922最后一次登陆后的等级'] < 30].index, inplace=True)
a = range(0, 1088)
df1.index = a
df = df1
df.isnull().sum()
#print(df.columns)

# del df['角色id']
df = df.drop('角色id', 1)

#print(df.columns)

for col in ['角色', 'isparty', 'ispvp',
            '玩家地区','whether_have_any_duanzao_+5']:
    le_hot = OneHotEncoder(sparse=False)
    hot_feature_arr = le_hot.fit_transform(df[[col]])

    hot_features = pd.DataFrame(hot_feature_arr)

    del df[col]

    hot_features.columns = le_hot.get_feature_names([col])

    # print (hot_features)

    df = pd.concat([df, hot_features], axis=1)
#print(df.columns)
df = df.drop(['战力', '仙门试炼最高级',  '首次炼魔阵等级',
       'duanzao+1', 'duanzao+2', 'duanzao+3', 'duanzao+4',
       'duanzao+5', 'duanzao+6', 'duanzao+7', 'duanzao+8', 'duanzao+9',
       'duanzao+10', 'duanzao+11', 'duanzao+12', 'duanzao+13',
              '玩家地区_Others', '玩家地区_加拿大',
              '玩家地区_巴西', '玩家地区_美国', '玩家地区_菲律宾','isforge'
              ], 1)
print(df.columns)
print(df)





# correlation
correlation = df.corr()
# tick labels
matrix_cols = correlation.columns.tolist()
# convert to array
corr_array = np.array(correlation)

# Plotting
trace = go.Heatmap(z=corr_array,
                   x=matrix_cols,
                   y=matrix_cols,
                   colorscale="Viridis",
                   colorbar=dict(title="Pearson Correlation coefficient",
                                 titleside="right"
                                 ),
                   )

layout = go.Layout(dict(title="Correlation Matrix for variables",
                        autosize=False,
                        height=1000,
                        width=1000,
                        margin=dict(r=0, l=210,
                                    t=25, b=210,
                                    ),
                        yaxis=dict(tickfont=dict(size=9)),
                        xaxis=dict(tickfont=dict(size=9))
                        )
                   )

data = [trace]
fig = go.Figure(data=data, layout=layout)
# py.iplot(fig)


# 提出所有feature列
feature_col_list = df.columns.tolist()
del feature_col_list[1]





train, test = train_test_split(df, test_size=DATASPLITRATE, random_state=111)

X_train = train[feature_col_list]
y_train = train['churn']

X_test = test[feature_col_list]
y_test = test['churn']

clf = tree.DecisionTreeClassifier(max_depth=MAXDEPTH )
# clf = tree.DecisionTreeClassifier(max_depth=MAXDEPTH,min_impurity_decrease=MINIMPURITYDECRESASE)

clf = clf.fit(X_train, y_train)
pred = clf.predict(X_test)

# print(classification_report(y_test, pred))
print('------------')
print(metrics.f1_score(y_test, pred))





train, test = train_test_split(df, test_size=DATASPLITRATE, random_state=111)

X_train = train[feature_col_list]
y_train = train['churn']

X_test = test[feature_col_list]
y_test = test['churn']

clf = tree.DecisionTreeClassifier(max_depth=MAXDEPTH)
#clf = tree.DecisionTreeClassifier(max_depth=MAXDEPTH,min_impurity_decrease=MINIMPURITYDECRESASE)



clf = clf.fit(X_train, y_train)
pred = clf.predict(X_test)

print(classification_report(y_test, pred))
print('------------')
print(metrics.f1_score(y_test,pred))

for feat, importance in zip(X_train.columns, clf.feature_importances_):
    if importance > 0:
        print('feature: {f}, importance: {i}'.format(f=feat, i=importance))

fig, axes = plt.subplots(figsize=(25, 25))

tree.plot_tree(clf, feature_names=feature_col_list)

#plt.show()




print(X_train.columns)


rf = RandomForestRegressor()
rf.fit(X_train, y_train)
pred = rf.predict(X_test)

print("Features sorted by their score:")
print(sorted(zip(map(lambda x: round(x, 4), rf.feature_importances_), X_train.columns), reverse=True))

list =  sorted(zip(map(lambda x: round(x, 4), rf.feature_importances_), X_train.columns), reverse=True)
list = pd.DataFrame(list)
print(list)


