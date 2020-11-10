# coding: utf-8
import datetime
from collections import Counter
import warnings

from sklearn.preprocessing import OneHotEncoder

warnings.filterwarnings('ignore')
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn import tree
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']  # 显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 这两行需要手动设置
from sklearn.metrics import confusion_matrix, accuracy_score, classification_report
from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics
import seaborn as sns

sns.set_style('darkgrid')
sns.set_palette('muted')

pd.set_option('display.max_rows', None)  # 可以填数字，填None表示'行'无限制
pd.set_option('display.max_columns', None)  # 可以填数字，填None表示'列'无限制
pd.set_option('display.width', 1000)  # 横向不换行

DATASPLITRATE = 0.2
MAXDEPTH = 4
# MINIMPURITYDECRESASE=val
MINSAMPLESSPLIT = 1

df = pd.read_csv('D:/data/final_data_10.csv')
#print(df.columns)
onehot_dflist = df.drop(['LOST'],1)
#df = df.drop(['dyLMZ','dyXMSL','dyLYQ','dyKTT','dyXMHJ','dySYZC','dyPTY','dyFBJL','dyFBRH','dyfee'],1)

for col in onehot_dflist:
    le_hot = OneHotEncoder(sparse=False)
    hot_feature_arr = le_hot.fit_transform(df[[col]])

    hot_features = pd.DataFrame(hot_feature_arr)

    del df[col]

    hot_features.columns = le_hot.get_feature_names([col])

    # print (hot_features)

    df = pd.concat([df, hot_features], axis=1)


internal_chars = df.columns.tolist()
corrmat = df[internal_chars].corr()
f, ax = plt.subplots(figsize=(10, 7))
plt.xticks(rotation='0')
sns.heatmap(corrmat, square=False, linewidths=.5, annot=True)

#
#
# # correlation
# correlation = df.corr()
# # tick labels
# matrix_cols = correlation.columns.tolist()
# # convert to array
# corr_array = np.array(correlation)
#
# # Plotting
# trace = go.Heatmap(z=corr_array,
#                    x=matrix_cols,
#                    y=matrix_cols,
#                    colorscale="Viridis",
#                    colorbar=dict(title="Pearson Correlation coefficient",
#                                  titleside="right"
#                                  ),
#                    )
#
# layout = go.Layout(dict(title="Correlation Matrix for variables",
#                         autosize=False,
#                         height=1000,
#                         width=1000,
#                         margin=dict(r=0, l=210,
#                                     t=25, b=210,
#                                     ),
#                         yaxis=dict(tickfont=dict(size=9)),
#                         xaxis=dict(tickfont=dict(size=9))
#                         )
#                    )
#
# data = [trace]
# fig = go.Figure(data=data, layout=layout)
# # py.iplot(fig)


# 提出所有feature列
feature_col_list = df.columns.tolist()
# print(feature_col_list)
del feature_col_list[0]

train, test = train_test_split(df, test_size=DATASPLITRATE, random_state=111)

X_train = train[feature_col_list]
y_train = train['LOST']

X_test = test[feature_col_list]
y_test = test['LOST']

clf = tree.DecisionTreeClassifier(max_depth=MAXDEPTH)
# clf = tree.DecisionTreeClassifier(max_depth=MAXDEPTH,min_impurity_decrease=MINIMPURITYDECRESASE)

clf = clf.fit(X_train, y_train)
pred = clf.predict(X_test)

print(classification_report(y_test, pred))
print('------------')
print(metrics.f1_score(y_test, pred))

for feat, importance in zip(feature_col_list, clf.feature_importances_):
    if importance > 0:
        print('feature: {f}, importance: {i}'.format(f=feat, i=importance))

fig, axes = plt.subplots(figsize=(25, 25))

tree.plot_tree(clf, feature_names=feature_col_list)

plt.show()

X, y = X_train, y_train

# print(X_train.columns)


rf = RandomForestRegressor()
rf.fit(X_train, y_train)
pred = rf.predict(X_test)

# print("Features sorted by their score:")
# print(sorted(zip(map(lambda x: round(x, 4), rf.feature_importances_), X_train.columns), reverse=True))

list = sorted(zip(map(lambda x: round(x, 4), rf.feature_importances_), X_train.columns), reverse=True)
list = pd.DataFrame(list)
print(list)
print('----------------------------------------')

print('--------------------')
print('linear regression feature importance')
# linear regression feature importance

from sklearn.linear_model import LinearRegression

# define dataset

# define the model
model = LinearRegression()
# fit the model
model.fit(X, y)
# get importance
importance = model.coef_
# summarize feature importance
for i, v in enumerate(importance):
    print('Feature:', feature_col_list[int(i)], 'Score: %.2f' % (v * 100))

# plot feature importance
plt.bar([x for x in range(len(importance))], importance)

# plt.title('linear regression feature importance')
plt.show()

print(len(feature_col_list))

print('--------------------')
print('logistic regression for feature importance')
# logistic regression for feature importance

from sklearn.linear_model import LogisticRegression
from matplotlib import pyplot

# define the model
model = LogisticRegression()
# fit the model
model.fit(X, y)
# get importance
importance = model.coef_[0]
# summarize feature importance
LRI = []
for i, v in enumerate(importance):
    print('Feature:', feature_col_list[int(i)], 'Score: %.2f' % (v * 100))
    LRI.append([feature_col_list[int(i)], v])
# plot feature importance
# plt.title('logistic regression for feature importance')
pyplot.bar([x for x in range(len(importance))], importance)
pyplot.show()

# print('--------------------')
# print('decision tree for feature importance on a regression problem')
# # decision tree for feature importance on a regression problem
# from sklearn.datasets import make_regression
# from sklearn.tree import DecisionTreeRegressor
# from matplotlib import pyplot
#
# # define the model
# model = DecisionTreeRegressor()
# # fit the model
# model.fit(X, y)
# # get importance
# importance = model.feature_importances_
# # summarize feature importance
# for i, v in enumerate(importance):
#     print('Feature:', feature_col_list[int(i)], 'Score: %.2f' % (v * 100))
# # plot feature importance
# # plt.title('decision tree for feature importance on a regression problem')
# pyplot.bar([x for x in range(len(importance))], importance)
# pyplot.show()

print('--------------------')
print('decision tree for feature importance on a classification problem')
# decision tree for feature importance on a classification problem
from sklearn.datasets import make_classification
from sklearn.tree import DecisionTreeClassifier
from matplotlib import pyplot

# define dataset
X, y = make_classification(n_samples=1000, n_features=10, n_informative=5, n_redundant=5, random_state=1)
# define the model
model = DecisionTreeClassifier()
# fit the model
model.fit(X, y)
# get importance
importance = model.feature_importances_
# summarize feature importance
for i, v in enumerate(importance):
    print('Feature:', feature_col_list[int(i)], 'Score: %.2f' % (v * 100))
# plot feature importance
# plt.title('decision tree for feature importance on a classification problem')
pyplot.bar([x for x in range(len(importance))], importance)
pyplot.show()

print(LRI)
print(LRI.sort)
