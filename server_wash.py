import datetime
from collections import Counter
import warnings

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

# 参数设定
dead_time = datetime.datetime.strptime(str('2020-10-14'), '%Y-%m-%d')
interval_time = datetime.timedelta(days=7)

# 模型参数
DATASPLITRATE = 0.2
MAXDEPTH = 4
# MINIMPURITYDECRESASE=val
MINSAMPLESSPLIT = 1

# 数据
print('正在加载数据')
print('loading...')
psn_lv_30 = pd.read_csv('D:/data/A5_30_offline.csv')
BH_ = pd.read_csv('D:/data/A5BH_1.csv')
CZ_ = pd.read_csv('D:/data/A5CZ_1.csv')
DZ_ = pd.read_csv('D:/data/A5DZ_1.csv')
KILL_ = pd.read_csv('D:/data/A5KILL_1.csv')
LMZ_ = pd.read_csv('D:/data/A5LMZ_1.csv')  # LOG_TYPE_LMZ
XMSL_ = pd.read_csv('D:/data/A5XMSL_1.csv')  # GAME_LOG_XIANMEN_TRAILS
LYQ_ = pd.read_csv('D:/data/A5LYQ_1.csv')  # CHILDLOG_LOG_SKYBOOK_ADD_TIMES_4
KTT_ = pd.read_csv('D:/data/A5KTT_1.csv')  # CHILDLOG_LOG_SKYBOOK_ADD_TIMES_7
XMHJ_ = pd.read_csv('D:/data/A5XMHJ_1.csv')  # CHILDLOG_LOG_SKYBOOK_ADD_TIMES_14
SYZC_ = pd.read_csv('D:/data/A5SYZC_1.csv')  # CHILDLOG_LOG_SKYBOOK_ADD_TIMES_13
PTY_ = pd.read_csv('D:/data/A5PTY_1.csv')  # CHILDLOG_LOG_ACT_START_10
FBJL_ = pd.read_csv('D:/data/A5FBJL_1.csv')  # LOG_COPY_ENDINFO
FBRH_ = pd.read_csv('D:/data/A5FBRH_1.csv')  # LOG_FABAO_FUSION
REGION_ = pd.read_csv('D:/data/A5region_1.csv')

# FML_ = pd.read_csv('D:/data/A5FML_1.csv')# LOG_COPY_FML
# TJCZ_ = pd.read_csv('D:/data/A5TJCZ_1.csv')# CHILDLOG_LOG_SKYBOOK_ADD_TIMES_23
# ZBXM_ = pd.read_csv('D:/data/A5ZBXM_1.csv')# CHILDLOG_LOG_SKYBOOK_ADD_TIMES_86
# GWWJ_ = pd.read_csv('D:/data/A5GWWJ_1.csv')# CHILDLOG_LOG_SKYBOOK_ADD_TIMES_73
# CDYH_ = pd.read_csv('D:/data/A5CDYH_1.csv')# CHILDLOG_LOG_SKYBOOK_ADD_TIMES_69
# MBMC_ = pd.read_csv('D:/data/A5MBMC_1.csv')# CHILDLOG_LOG_SKYBOOK_ADD_TIMES_5
# FBXL_ = pd.read_csv('D:/data/A5FBXL_1.csv')# LOG_FABAO_REFINE


print('数据加载完毕')

print('开始清洗数据')
print('loading...')
# 大于30
lost_people_ = []
for n in psn_lv_30['u64_1'].unique().tolist():
    last_time_str = psn_lv_30[psn_lv_30['u64_1'] == n].sort_values(by='wash_date').iloc[-1]['wash_date']
    last_time = datetime.datetime.strptime(last_time_str, '%Y-%m-%d')
    if (dead_time - last_time) >= interval_time:
        lost_people_.append(n)

psn_lv_30 = pd.DataFrame(psn_lv_30['u64_1'].unique().tolist())
psn_lv_30.rename(columns={0: 'player_id'}, inplace=True)
psn_lost = pd.DataFrame(lost_people_)
psn_lost.rename(columns={0: 'player_id'}, inplace=True)
psn_lost['LOST'] = 1
final_data = psn_lv_30.join(psn_lost.set_index('player_id'), on='player_id', how='left')

# 帮会
BH_['isparty'] = 1
final_data = final_data.join(BH_.set_index('player_id'), on='player_id', how='left')

# 充值
CZ_[CZ_['player_id'].isin(psn_lv_30['player_id'].tolist())]
CZ_.drop_duplicates(subset='player_id')
CZ_ = CZ_.groupby(by=['player_id'])['fee'].sum().to_frame()
CZ_['player_id'] = CZ_.index
CZ_ = CZ_.reset_index(drop=True)
final_data = final_data.join(CZ_.set_index('player_id'), on='player_id', how='left')

# 锻造次数
DZ_ = DZ_[DZ_['u64_1'].isin(final_data['player_id'].tolist())]
DZ_['u64_1'].unique()
dict = {}

for key in DZ_['u64_1'].tolist():
    dict[key] = dict.get(key, 0) + 1

forge_time = pd.DataFrame.from_dict(dict, orient='index')
forge_time.columns = ['Forge_time']
forge_time.iloc[0].name
forge_time['角色id'] = ''

for n in range(len(forge_time)):
    forge_time.iloc[n, 1] = forge_time.iloc[n].name

forge_time = forge_time.reset_index().drop(['index'], 1)
forge_time.rename(columns={'角色id': 'player_id'}, inplace=True)

final_data = final_data.join(forge_time.set_index('player_id'), on='player_id', how='left')

# 锻造成功率
rate1 = []
for n in DZ_['u64_1'].unique().tolist():
    rate1.append(DZ_[DZ_['u64_1'] == n]['u32_8'].iloc[-1] / 100)

rate1 = pd.DataFrame(rate1)
new = pd.DataFrame(DZ_['u64_1'].unique().tolist(), columns=['角色id'])

new['rate'] = rate1[0]

new.rename(columns={'角色id': 'player_id'}, inplace=True)
final_data = final_data.join(new.set_index('player_id'), on='player_id', how='left')

# 杀人仇杀

KILL_['u64_1'].tolist()

result = Counter(KILL_['u64_1'].tolist())

# u64_1 被杀统计
dict = {}
for key in KILL_['u64_1'].tolist():
    dict[key] = dict.get(key, 0) + 1

killed = dict

# u64_2 杀人统计
dict = {}
for key in KILL_['u64_2'].tolist():
    dict[key] = dict.get(key, 0) + 1

kill = dict
df_kill = pd.DataFrame.from_dict(kill, orient='index')
df_killed = pd.DataFrame.from_dict(killed, orient='index')

df_kill['player_id'] = df_kill.index
df_kill = df_kill.reset_index(drop=True)
df_kill.rename(columns={0: 'killtimes'}, inplace=True)

df_killed['player_id'] = df_killed.index
df_killed = df_killed.reset_index(drop=True)
df_killed.rename(columns={0: 'killedtimes'}, inplace=True)

final_data = final_data.join(df_kill.set_index('player_id'), on='player_id', how='left')
final_data = final_data.join(df_killed.set_index('player_id'), on='player_id', how='left')

# 炼魔阵
LMZ_ = LMZ_[LMZ_['u64_1'].isin(final_data['player_id'].tolist())]
LMZ_.rename(columns={'u64_1': 'player_id', 'count': 'LMZCOUNT'}, inplace=True)
final_data = final_data.join(LMZ_.set_index('player_id'), on='player_id', how='left')
final_data['dyLMZ'] = final_data['LMZCOUNT'] > final_data['LMZCOUNT'].mean(0)
final_data['dyLMZ'] = final_data['dyLMZ'] + 0
final_data['isLMZ'] = final_data['LMZCOUNT'] > 0
final_data['isLMZ'] = final_data['isLMZ'] + 0

# 仙门试炼

XMSL_ = XMSL_[XMSL_['u64_1'].isin(final_data['player_id'].tolist())]

XMSL_.rename(columns={'u64_1': 'player_id', 'count': 'XMSLCOUNT'}, inplace=True)
final_data = final_data.join(XMSL_.set_index('player_id'), on='player_id', how='left')
final_data['dyXMSL'] = final_data['XMSLCOUNT'] > final_data['XMSLCOUNT'].mean(0)
final_data['dyXMSL'] = final_data['dyXMSL'] + 0
final_data['isXMSL'] = final_data['XMSLCOUNT'] > 0
final_data['isXMSL'] = final_data['isXMSL'] + 0

# 李英琼

LYQ_ = LYQ_[LYQ_['u64_1'].isin(final_data['player_id'].tolist())]
LYQ_.rename(columns={'u64_1': 'player_id', 'count': 'LYQCOUNT'}, inplace=True)
final_data = final_data.join(LYQ_.set_index('player_id'), on='player_id', how='left')
final_data['dyLYQ'] = final_data['LYQCOUNT'] > final_data['LYQCOUNT'].mean(0)
final_data['dyLYQ'] = final_data['dyLYQ'] + 0
final_data['isLYQ'] = final_data['LYQCOUNT'] > 0
final_data['isLYQ'] = final_data['isLYQ'] + 0

# 苦头托

KTT_ = KTT_[KTT_['u64_1'].isin(final_data['player_id'].tolist())]
KTT_.rename(columns={'u64_1': 'player_id', 'count': 'KTTCOUNT'}, inplace=True)
final_data = final_data.join(KTT_.set_index('player_id'), on='player_id', how='left')
final_data['dyKTT'] = final_data['KTTCOUNT'] > final_data['KTTCOUNT'].mean(0)
final_data['dyKTT'] = final_data['dyKTT'] + 0
final_data['isKTT'] = final_data['KTTCOUNT'] > 0
final_data['isKTT'] = final_data['isKTT'] + 0

# 血魔幻境

XMHJ_ = XMHJ_[XMHJ_['u64_1'].isin(final_data['player_id'].tolist())]
XMHJ_.rename(columns={'u64_1': 'player_id', 'count': 'XMHJCOUNT'}, inplace=True)
final_data = final_data.join(XMHJ_.set_index('player_id'), on='player_id', how='left')
final_data['dyXMHJ'] = final_data['XMHJCOUNT'] > final_data['XMHJCOUNT'].mean(0)
final_data['dyXMHJ'] = final_data['dyXMHJ'] + 0
final_data['isXMHJ'] = final_data['XMHJCOUNT'] > 0
final_data['isXMHJ'] = final_data['isXMHJ'] + 0

# 圣焰战场

SYZC_ = SYZC_[SYZC_['u64_1'].isin(final_data['player_id'].tolist())]
SYZC_.rename(columns={'u64_1': 'player_id', 'count': 'SYZCCOUNT'}, inplace=True)
final_data = final_data.join(SYZC_.set_index('player_id'), on='player_id', how='left')
final_data['dySYZC'] = final_data['SYZCCOUNT'] > final_data['SYZCCOUNT'].mean(0)
final_data['dySYZC'] = final_data['dySYZC'] + 0
final_data['isSYZC'] = final_data['SYZCCOUNT'] > 0
final_data['isSYZC'] = final_data['isSYZC'] + 0

# 蟠桃园

PTY_ = PTY_[PTY_['u64_1'].isin(final_data['player_id'].tolist())]
PTY_.rename(columns={'u64_1': 'player_id', 'count': 'PTYCOUNT'}, inplace=True)
final_data = final_data.join(PTY_.set_index('player_id'), on='player_id', how='left')
final_data['dyPTY'] = final_data['PTYCOUNT'] > final_data['PTYCOUNT'].mean(0)
final_data['dyPTY'] = final_data['dyPTY'] + 0
final_data['isPTY'] = final_data['PTYCOUNT'] > 0
final_data['isPTY'] = final_data['isPTY'] + 0
# 副本

FBJL_ = FBJL_[FBJL_['u64_1'].isin(final_data['player_id'].tolist())]
FBJL_.rename(columns={'u64_1': 'player_id', 'count': 'FBJLCOUNT'}, inplace=True)
final_data = final_data.join(FBJL_.set_index('player_id'), on='player_id', how='left')
final_data['dyFBJL'] = final_data['FBJLCOUNT'] > final_data['FBJLCOUNT'].mean(0)
final_data['dyFBJL'] = final_data['dyFBJL'] + 0
final_data['isFBJL'] = final_data['FBJLCOUNT'] > 0
final_data['isFBJL'] = final_data['isFBJL'] + 0

# # 封魔录
#
# FML_ = FML_[FML_['u64_1'].isin(final_data['player_id'].tolist())]
# FML_.rename(columns={'u64_1': 'player_id', 'count': 'FMLCOUNT'}, inplace=True)
# final_data = final_data.join(FML_.set_index('player_id'), on='player_id', how='left')
# final_data['dyFML'] = final_data['FMLCOUNT'] > final_data['FMLCOUNT'].mean(0)
# final_data['dyFML'] = final_data['dyFML'] + 0
# final_data['isFML'] = final_data['FMLCOUNT'] > 0
# final_data['isFML'] = final_data['isFML'] + 0
# # 天降彩珠
#
# TJCZ_ = TJCZ_[TJCZ_['u64_1'].isin(final_data['player_id'].tolist())]
# TJCZ_.rename(columns={'u64_1': 'player_id', 'count': 'TJCZCOUNT'}, inplace=True)
# final_data = final_data.join(TJCZ_.set_index('player_id'), on='player_id', how='left')
# final_data['dyTJCZ'] = final_data['TJCZCOUNT'] > final_data['TJCZCOUNT'].mean(0)
# final_data['dyTJCZ'] = final_data['dyTJCZ'] + 0
# final_data['isTJCZ'] = final_data['TJCZCOUNT'] > 0
# final_data['isTJCZ'] = final_data['isTJCZ'] + 0
#
# # 追捕邪魔
#
# ZBXM_ = ZBXM_[ZBXM_['u64_1'].isin(final_data['player_id'].tolist())]
# ZBXM_.rename(columns={'u64_1': 'player_id', 'count': 'ZBXMCOUNT'}, inplace=True)
# final_data = final_data.join(ZBXM_.set_index('player_id'), on='player_id', how='left')
# final_data['dyZBXM'] = final_data['ZBXMCOUNT'] > final_data['ZBXMCOUNT'].mean(0)
# final_data['dyZBXM'] = final_data['dyZBXM'] + 0
# final_data['isZBXM'] = final_data['ZBXMCOUNT'] > 0
# final_data['isZBXM'] = final_data['isZBXM'] + 0
#
# # 怪物围剿
#
# GWWJ_ = GWWJ_[GWWJ_['u64_1'].isin(final_data['player_id'].tolist())]
# GWWJ_.rename(columns={'u64_1': 'player_id', 'count': 'GWWJCOUNT'}, inplace=True)
# final_data = final_data.join(GWWJ_.set_index('player_id'), on='player_id', how='left')
# final_data['dyGWWJ'] = final_data['GWWJCOUNT'] > final_data['GWWJCOUNT'].mean(0)
# final_data['dyGWWJ'] = final_data['dyGWWJ'] + 0
# final_data['isGWWJ'] = final_data['GWWJCOUNT'] > 0
# final_data['isGWWJ'] = final_data['isGWWJ'] + 0

# # 超度游魂
#
# CDYH_ = CDYH_[CDYH_['u64_1'].isin(final_data['player_id'].tolist())]
# CDYH_.rename(columns={'u64_1': 'player_id', 'count': 'CDYHCOUNT'}, inplace=True)
# final_data = final_data.join(CDYH_.set_index('player_id'), on='player_id', how='left')
# final_data['dyCDYH'] = final_data['CDYHCOUNT'] > final_data['CDYHCOUNT'].mean(0)
# final_data['dyCDYH'] = final_data['dyCDYH'] + 0
# final_data['isCDYH'] = final_data['CDYHCOUNT'] > 0
# final_data['isCDYH'] = final_data['isCDYH'] + 0
#
# # 秘宝蒙尘
#
# MBMC_ = MBMC_[MBMC_['u64_1'].isin(final_data['player_id'].tolist())]
# MBMC_.rename(columns={'u64_1': 'player_id', 'count': 'MBMCCOUNT'}, inplace=True)
# final_data = final_data.join(MBMC_.set_index('player_id'), on='player_id', how='left')
# final_data['dyMBMC'] = final_data['MBMCCOUNT'] > final_data['MBMCCOUNT'].mean(0)
# final_data['dyMBMC'] = final_data['dyMBMC'] + 0
# final_data['isMBMC'] = final_data['MBMCCOUNT'] > 0
# final_data['isMBMC'] = final_data['isMBMC'] + 0
#
# # 法宝修炼
#
# FBXL_ = FBXL_[FBXL_['u64_1'].isin(final_data['player_id'].tolist())]
# FBXL_.rename(columns={'u64_1': 'player_id', 'count': 'FBXLCOUNT'}, inplace=True)
# final_data = final_data.join(FBXL_.set_index('player_id'), on='player_id', how='left')
# final_data['dyFBXL'] = final_data['FBXLCOUNT'] > final_data['FBXLCOUNT'].mean(0)
# final_data['dyFBXL'] = final_data['dyFBXL'] + 0
# final_data['isFBXL'] = final_data['FBXLCOUNT'] > 0
# final_data['isFBXL'] = final_data['isFBXL'] + 0

# 法宝融合

FBRH_ = FBRH_[FBRH_['u64_1'].isin(final_data['player_id'].tolist())]
FBRH_.rename(columns={'u64_1': 'player_id', 'count': 'FBRHCOUNT'}, inplace=True)
final_data = final_data.join(FBRH_.set_index('player_id'), on='player_id', how='left')
final_data['dyFBRH'] = final_data['FBRHCOUNT'] > final_data['FBRHCOUNT'].mean(0)
final_data['dyFBRH'] = final_data['dyFBRH'] + 0
final_data['isFBRH'] = final_data['FBRHCOUNT'] > 0
final_data['isFBRH'] = final_data['isFBRH'] + 0

# 帮贡
final_data[final_data['banggong'] > 0]['banggong'].hist(bins=100)
# plt.title('banggong')
# plt.show()
# print(final_data['banggong'].max(),final_data[final_data['banggong']>0]['banggong'].min())
# print(final_data[final_data['banggong']>0]['banggong'].mean(0),final_data[final_data['banggong']>0]['banggong'].median(0))
final_data['dybanggong'] = final_data['banggong'] > final_data[final_data['banggong'] > 0]['banggong'].mean(0)
final_data['dybanggong'] = final_data['dybanggong'] + 0

# 费用
final_data[final_data['fee'] > 0]['fee'].hist(bins=100)
# plt.title('fee')
# plt.show()
# print(final_data['fee'].max(),final_data[final_data['fee']>0]['fee'].min())
# print(final_data[final_data['fee']>0]['fee'].mean(0),final_data[final_data['fee']>0]['fee'].median(0))
final_data['dyfee'] = final_data['fee'] > final_data[final_data['fee'] > 0]['fee'].mean(0)
final_data['dyfee'] = final_data['dyfee'] + 0
final_data['isfee'] = final_data['fee'] > 0
final_data['isfee'] = final_data['dyfee'] + 0

# 锻造次数
final_data[final_data['Forge_time'] > 0]['Forge_time'].hist(bins=100)
# plt.title('forge_time')
# plt.show()
# print(final_data['Forge_time'].max(),final_data[final_data['Forge_time']>0]['Forge_time'].min())
# print(final_data[final_data['Forge_time']>0]['Forge_time'].mean(0),final_data[final_data['Forge_time']>0]['Forge_time'].median(0))
final_data['dyForge_time'] = final_data['Forge_time'] > final_data[final_data['fee'] > 0]['Forge_time'].mean(0)
final_data['dyForge_time'] = final_data['dyForge_time'] + 0

# #rate
# final_data[final_data['rate']>0]['rate'].hist(bins=100)
# plt.title('rate')
# plt.show()
# print(final_data['rate'].max(),final_data[final_data['rate']>0]['rate'].min())
# print(final_data[final_data['rate']>0]['rate'].mean(0),final_data[final_data['rate']>0]['rate'].median(0))
# final_data['dyrate'] = final_data['rate'] > final_data[final_data['rate']>0]['rate'].mean(0)
# final_data['dyrate'] = final_data['dyrate'] +0
# rate
final_data[final_data['rate'] > 0]['rate'].hist(bins=100)
# plt.title('rate')
# plt.show()
# print(final_data['rate'].max(),final_data['rate'].min())
# print(final_data['rate'].mean(0),final_data['rate'].median(0))
final_data['dyrate'] = final_data['rate'] > final_data['rate'].mean(0)
final_data['dyrate'] = final_data['dyrate'] + 0

# killtimes
final_data[final_data['killtimes'] > 0]['killtimes'].hist(bins=100)
# plt.title('killtimes')
# plt.show()
# print(final_data['killtimes'].max(),final_data['killtimes'].min())
# print(final_data['killtimes'].mean(0),final_data['killtimes'].median(0))
final_data['dykilltimes'] = final_data['killtimes'] > final_data['killtimes'].mean(0)
final_data['dykilltimes'] = final_data['dykilltimes'] + 0

# killedtimes
final_data[final_data['killedtimes'] > 0]['killedtimes'].hist(bins=100)
# plt.title('killedtimes')
# plt.show()
# print(final_data['killedtimes'].max(),final_data['killedtimes'].min())
# print(final_data['killedtimes'].mean(0),final_data['killedtimes'].median(0))
final_data['dykilledtimes'] = final_data['killedtimes'] > final_data['killedtimes'].mean(0)
final_data['dykilledtimes'] = final_data['dykilledtimes'] + 0

final_data = final_data.drop(
    ['player_id', 'banggong', 'fee', 'Forge_time', 'rate', 'killtimes', 'killedtimes', 'LMZCOUNT', 'XMSLCOUNT',
     'LYQCOUNT', 'KTTCOUNT', 'XMHJCOUNT', 'SYZCCOUNT', 'PTYCOUNT', 'FBJLCOUNT', 'FBRHCOUNT'], 1)
final_data.fillna(0, inplace=True)
print(final_data.shape)
final_data.to_csv('D:/data/final_data_5.csv', index=False)
print('数据清洗完毕')
print('生成文件完成')
