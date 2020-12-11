# coding: utf-8
import datetime
from collections import Counter
import warnings

warnings.filterwarnings('ignore')
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn import tree
import matplotlib.pyplot as plt
import pymysql
import psycopg2

plt.rcParams['font.sans-serif'] = ['SimHei']  # 显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 这两行需要手动设置
from sklearn.metrics import confusion_matrix, accuracy_score, classification_report
from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics
import seaborn as sns
from sklearn import preprocessing
import numpy as np

sns.set_style('darkgrid')
sns.set_palette('muted')

# pd.set_option('display.max_rows', None)  # 可以填数字，填None表示'行'无限制
# pd.set_option('display.max_columns', None)  # 可以填数字，填None表示'列'无限制

# pd.set_option('display.width', 1000)  # 横向不换行

from sklearn.preprocessing import OneHotEncoder

startdate = '2020-09-15'
enddate = '2020-10-14'
final_result = []
serverid = 'glb00011'
for t in range(0, (
        datetime.datetime.strptime(enddate, '%Y-%m-%d') - datetime.datetime.strptime(startdate, '%Y-%m-%d')).days):

    ## 数据参数
    # dead_time = datetime.datetime.strptime(str('2020-10-14'), '%Y-%m-%d')
    interval_time = datetime.timedelta(days=7)


    # t = 7
    seconddate = datetime.datetime.strftime(
        (datetime.datetime.strptime(startdate, '%Y-%m-%d') + datetime.timedelta(days=1)), '%Y-%m-%d')
    thirddate = datetime.datetime.strftime(
        (datetime.datetime.strptime(startdate, '%Y-%m-%d') + datetime.timedelta(days=2)),
        '%Y-%m-%d')
    sevendate = datetime.datetime.strftime(
        (datetime.datetime.strptime(startdate, '%Y-%m-%d') + datetime.timedelta(days=6)),
        '%Y-%m-%d')
    tdate = datetime.datetime.strftime((datetime.datetime.strptime(startdate, '%Y-%m-%d') + datetime.timedelta(days=t)),
                                       '%Y-%m-%d')
    PG_HOST = "192.168.2.154"
    PG_PORT = "5432"
    PG_USER = "cyread"
    PG_PASSWORD = "oS8lckyVT9q9q0qr"
    PG_DATABASE = "db_ana_pss_glb"

    MYSQL_HOST = "glb-washerdata.mysql.database.azure.com"
    MYSQL_PORT = 3306
    MYSQL_USER = "gmdb@glb-washerdata"
    MYSQL_PASSWORD = "PLrGisMS0HRaFE7f"
    MYSQL_DATABASE = "db_ana_t11"

    ## 模型参数

    DATASPLITRATE = 0.2
    MAXDEPTH = 4
    # MINIMPURITYDECRESASE=val
    MINSAMPLESSPLIT = 1

    ## 数据清洗

    # 大于30级的玩家
    sql_dayu30 = f"select distinct u64_1  from game_log_pss_final glpf where log_type_name ='LOG_PLAYER_LEVEL_UP' and u32_1 >=30 and server = '{serverid}' and wash_date between '{startdate}' and '{enddate}' order by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_dayu30)

    data_dayu30 = curs.fetchall()
    columns_ = ['u64_1']

    data_dayu30 = pd.DataFrame(data_dayu30, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()

    final_data = data_dayu30

    # 第一天登录的玩家
    sql_1login = f"select distinct u64_1 from game_log_pss_final glpf where log_type_name ='LOG_TYPE_PLAYER_LOGIN' and server ='{serverid}' and wash_date ='{startdate}'"

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_1login)

    sql_1login = curs.fetchall()
    columns_ = ['u64_1']

    data_1login = pd.DataFrame(sql_1login, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()

    data_dy30_1l = data_dayu30[data_dayu30['u64_1'].isin(data_1login['u64_1'].tolist())]
    data_dy30_1l['1login'] = 1
    final_data = final_data.join(data_dy30_1l.set_index('u64_1'), on='u64_1', how='left')

    # 第t天登陆的玩家
    sql_tlogin = f"select distinct u64_1 from game_log_pss_final glpf where log_type_name ='LOG_TYPE_PLAYER_LOGIN' and server ='{serverid}' and wash_date ='{tdate}'"

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_tlogin)

    sql_tlogin = curs.fetchall()
    columns_ = ['u64_1']

    data_tlogin = pd.DataFrame(sql_tlogin, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()

    data_tlogin['tlogin'] = 1
    final_data = final_data.join(data_tlogin.set_index('u64_1'), on='u64_1', how='left')

    final_data_tsave = final_data[(final_data['1login'] == 1) & (final_data['tlogin'] == 1)]['u64_1']
    final_data_tsave = pd.DataFrame(final_data_tsave)
    final_data_tsave['tsave'] = 1
    final_data = final_data.join(final_data_tsave.set_index('u64_1'), on='u64_1', how='left')
    final_data.drop(['tlogin'], 1)

    # 帮会玩家
    sql_bh = f"select player_id ,banggong from player_last_info where acct in (select distinct customer_user_id  from acct.af_log al where event_name ='af_achieve_joingang'  and event_time between '{startdate}' and '{enddate}') and server = '{serverid}'"

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_bh)

    data_bh = curs.fetchall()
    columns_ = ['u64_1', 'banggong']

    data_bh = pd.DataFrame(data_bh, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()

    data_bh['isparty'] = 1
    final_data = final_data.join(data_bh.set_index('u64_1'), on='u64_1', how='left')

    # 充值
    sql_cz = f"select fee,ch,player_id from db_ana_t11.point_log_detail where server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' order by player_id asc "

    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        passwd=MYSQL_PASSWORD,
        db='db_ana_t11',
    )

    # 通过获取到的数据库连接conn下的cursor()方法来创建游标。
    cur = conn.cursor()

    # 创建数据表,通过游标cur 操作execute()方法可以写入纯sql语句。通过execute()方法中写如sql语句来对数据进行操作

    # print(sql_)
    cur.execute(sql_cz)
    data_cz = cur.fetchall()
    # print(rows_)
    # zc_table.append(rows_)
    # cur.close()
    cur.close()

    # conn.commit()方法在提交事物，在向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入。
    conn.commit()

    # conn.close()关闭数据库连接
    conn.close()

    data_cz = pd.DataFrame(data_cz, columns=('fee', 'channel', 'u64_1'))

    data_cz[data_cz['u64_1'].isin(final_data['u64_1'].tolist())]
    data_cz.drop_duplicates(subset='u64_1')
    data_cz = data_cz.groupby(by=['u64_1'])['fee'].sum().to_frame()
    data_cz['u64_1'] = data_cz.index
    data_cz = data_cz.reset_index(drop=True)
    final_data = final_data.join(data_cz.set_index('u64_1'), on='u64_1', how='left')

    # 锻造
    sql_dz = f"select u64_1,u32_8 from game_log_pss_final where log_type_name ='LOG_FORGE' and wash_date between '{startdate}' and '{enddate}' and  server ='{serverid}' "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_dz)

    data_dz = curs.fetchall()
    columns_ = ['u64_1', 'u32_8']

    data_dz = pd.DataFrame(data_dz, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()

    # 锻造次数
    data_dz = data_dz[data_dz['u64_1'].isin(final_data['u64_1'].tolist())]
    data_dz['u64_1'].unique()
    dict = {}

    for key in data_dz['u64_1'].tolist():
        dict[key] = dict.get(key, 0) + 1

    forge_time = pd.DataFrame.from_dict(dict, orient='index')
    forge_time.columns = ['Forge_time']
    forge_time.iloc[0].name
    forge_time['角色id'] = ''

    for n in range(len(forge_time)):
        forge_time.iloc[n, 1] = forge_time.iloc[n].name

    forge_time = forge_time.reset_index().drop(['index'], 1)
    forge_time.rename(columns={'角色id': 'u64_1'}, inplace=True)

    final_data = final_data.join(forge_time.set_index('u64_1'), on='u64_1', how='left')

    # 锻造成功率
    rate1 = []
    for n in data_dz['u64_1'].unique().tolist():
        rate1.append(data_dz[data_dz['u64_1'] == n]['u32_8'].iloc[-1] / 100)

    rate1 = pd.DataFrame(rate1)
    new = pd.DataFrame(data_dz['u64_1'].unique().tolist(), columns=['u64_1'])

    new['rate'] = rate1[0]

    # new.rename(columns={'角色id': 'u64_1'}, inplace=True)
    final_data = final_data.join(new.set_index('u64_1'), on='u64_1', how='left')

    # 杀人仇杀
    sql_kill = f"select u64_1 ,u64_2 from game_log_pss_final where log_type_name ='LOG_ENEMY_KILL_INFO' and  server ='{serverid}'  and wash_date between '{startdate}' and '{enddate}' "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_kill)

    data_kill = curs.fetchall()
    columns_ = ['u64_1', 'u64_2']

    data_kill = pd.DataFrame(data_kill, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()

    # 杀人仇杀

    data_kill['u64_1'].tolist()

    result = Counter(data_kill['u64_1'].tolist())

    # u64_1 被杀统计
    dict = {}
    for key in data_kill['u64_1'].tolist():
        dict[key] = dict.get(key, 0) + 1

    killed = dict

    # u64_2 杀人统计
    dict = {}
    for key in data_kill['u64_2'].tolist():
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

    final_data = final_data.join(df_kill.set_index('player_id'), on='u64_1', how='left')
    final_data = final_data.join(df_killed.set_index('player_id'), on='u64_1', how='left')

    final_data.rename(columns={'u64_1': 'player_id'}, inplace=True)

    # 炼魔阵
    sql_lmz = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='LOG_TYPE_LMZ' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_lmz)

    data_lmz = curs.fetchall()
    columns_ = ['u64_1', 'count']

    LMZ_ = pd.DataFrame(data_lmz, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()

    LMZ_ = LMZ_[LMZ_['u64_1'].isin(final_data['player_id'].tolist())]
    LMZ_.rename(columns={'u64_1': 'player_id', 'count': 'LMZCOUNT'}, inplace=True)
    final_data = final_data.join(LMZ_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyLMZ'] = final_data['LMZCOUNT'] > final_data['LMZCOUNT'].mean(0)
    # final_data['dyLMZ'] = final_data['dyLMZ'] + 0
    # final_data['isLMZ'] = final_data['LMZCOUNT'] > 0
    # final_data['isLMZ'] = final_data['isLMZ'] + 0

    # 仙门试炼
    sql_xmsl = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='GAME_LOG_XIANMEN_TRAILS' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_xmsl)

    data_xmsl = curs.fetchall()
    columns_ = ['u64_1', 'count']

    XMSL_ = pd.DataFrame(data_xmsl, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()
    XMSL_ = XMSL_[XMSL_['u64_1'].isin(final_data['player_id'].tolist())]
    XMSL_.rename(columns={'u64_1': 'player_id', 'count': 'XMSLCOUNT'}, inplace=True)
    final_data = final_data.join(XMSL_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyXMSL'] = final_data['XMSLCOUNT'] > final_data['XMSLCOUNT'].mean(0)
    # final_data['dyXMSL'] = final_data['dyXMSL'] + 0
    # final_data['isXMSL'] = final_data['XMSLCOUNT'] > 0
    # final_data['isXMSL'] = final_data['isXMSL'] + 0

    # 李英琼
    sql_lyq = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='CHILDLOG_LOG_SKYBOOK_ADD_TIMES_4' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_lyq)

    data_lyq = curs.fetchall()
    columns_ = ['u64_1', 'count']

    LYQ_ = pd.DataFrame(data_lyq, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()
    LYQ_ = LYQ_[LYQ_['u64_1'].isin(final_data['player_id'].tolist())]
    LYQ_.rename(columns={'u64_1': 'player_id', 'count': 'LYQCOUNT'}, inplace=True)
    final_data = final_data.join(LYQ_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyLYQ'] = final_data['LYQCOUNT'] > final_data['LYQCOUNT'].mean(0)
    # final_data['dyLYQ'] = final_data['dyLYQ'] + 0
    # final_data['isLYQ'] = final_data['LYQCOUNT'] > 0
    # final_data['isLYQ'] = final_data['isLYQ'] + 0

    # 苦头托
    sql_ktt = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='CHILDLOG_LOG_SKYBOOK_ADD_TIMES_7' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_ktt)

    data_ktt = curs.fetchall()
    columns_ = ['u64_1', 'count']

    KTT_ = pd.DataFrame(data_ktt, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()

    KTT_ = KTT_[KTT_['u64_1'].isin(final_data['player_id'].tolist())]
    KTT_.rename(columns={'u64_1': 'player_id', 'count': 'KTTCOUNT'}, inplace=True)
    final_data = final_data.join(KTT_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyKTT'] = final_data['KTTCOUNT'] > final_data['KTTCOUNT'].mean(0)
    # final_data['dyKTT'] = final_data['dyKTT'] + 0
    # final_data['isKTT'] = final_data['KTTCOUNT'] > 0
    # final_data['isKTT'] = final_data['isKTT'] + 0

    # 血魔幻境
    sql_xmhj = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='CHILDLOG_LOG_SKYBOOK_ADD_TIMES_14' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_xmhj)

    data_xmhj = curs.fetchall()
    columns_ = ['u64_1', 'count']

    XMHJ_ = pd.DataFrame(data_xmhj, columns=columns_)

    conn.commit()
    curs.close()
    conn.close()
    XMHJ_ = XMHJ_[XMHJ_['u64_1'].isin(final_data['player_id'].tolist())]
    XMHJ_.rename(columns={'u64_1': 'player_id', 'count': 'XMHJCOUNT'}, inplace=True)
    final_data = final_data.join(XMHJ_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyXMHJ'] = final_data['XMHJCOUNT'] > final_data['XMHJCOUNT'].mean(0)
    # final_data['dyXMHJ'] = final_data['dyXMHJ'] + 0
    # final_data['isXMHJ'] = final_data['XMHJCOUNT'] > 0
    # final_data['isXMHJ'] = final_data['isXMHJ'] + 0

    # 圣焰战场
    sql_syzc = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='CHILDLOG_LOG_SKYBOOK_ADD_TIMES_13' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_syzc)

    data_syzc = curs.fetchall()
    columns_ = ['u64_1', 'count']

    SYZC_ = pd.DataFrame(data_syzc, columns=columns_)

    SYZC_ = SYZC_[SYZC_['u64_1'].isin(final_data['player_id'].tolist())]
    SYZC_.rename(columns={'u64_1': 'player_id', 'count': 'SYZCCOUNT'}, inplace=True)
    final_data = final_data.join(SYZC_.set_index('player_id'), on='player_id', how='left')
    # final_data['dySYZC'] = final_data['SYZCCOUNT'] > final_data['SYZCCOUNT'].mean(0)
    # final_data['dySYZC'] = final_data['dySYZC'] + 0
    # final_data['isSYZC'] = final_data['SYZCCOUNT'] > 0
    # final_data['isSYZC'] = final_data['isSYZC'] + 0

    # 蟠桃园
    sql_pty = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='CHILDLOG_LOG_ACT_START_10' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_pty)

    data_pty = curs.fetchall()
    columns_ = ['u64_1', 'count']

    PTY_ = pd.DataFrame(data_pty, columns=columns_)

    PTY_ = PTY_[PTY_['u64_1'].isin(final_data['player_id'].tolist())]
    PTY_.rename(columns={'u64_1': 'player_id', 'count': 'PTYCOUNT'}, inplace=True)
    final_data = final_data.join(PTY_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyPTY'] = final_data['PTYCOUNT'] > final_data['PTYCOUNT'].mean(0)
    # final_data['dyPTY'] = final_data['dyPTY'] + 0
    # final_data['isPTY'] = final_data['PTYCOUNT'] > 0
    # final_data['isPTY'] = final_data['isPTY'] + 0

    # 副本
    sql_fbjl = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='LOG_COPY_ENDINFO' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_fbjl)

    data_fbjl = curs.fetchall()
    columns_ = ['u64_1', 'count']

    FBJL_ = pd.DataFrame(data_fbjl, columns=columns_)

    FBJL_ = FBJL_[FBJL_['u64_1'].isin(final_data['player_id'].tolist())]
    FBJL_.rename(columns={'u64_1': 'player_id', 'count': 'FBJLCOUNT'}, inplace=True)
    final_data = final_data.join(FBJL_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyFBJL'] = final_data['FBJLCOUNT'] > final_data['FBJLCOUNT'].mean(0)
    # final_data['dyFBJL'] = final_data['dyFBJL'] + 0
    # final_data['isFBJL'] = final_data['FBJLCOUNT'] > 0
    # final_data['isFBJL'] = final_data['isFBJL'] + 0

    # 封魔录
    sql_fml = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='LOG_COPY_FML' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_fml)

    data_fml = curs.fetchall()
    columns_ = ['u64_1', 'count']

    FML_ = pd.DataFrame(data_fml, columns=columns_)

    FML_ = FML_[FML_['u64_1'].isin(final_data['player_id'].tolist())]
    FML_.rename(columns={'u64_1': 'player_id', 'count': 'FMLCOUNT'}, inplace=True)
    final_data = final_data.join(FML_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyFML'] = final_data['FMLCOUNT'] > final_data['FMLCOUNT'].mean(0)
    # final_data['dyFML'] = final_data['dyFML'] + 0
    # final_data['isFML'] = final_data['FMLCOUNT'] > 0
    # final_data['isFML'] = final_data['isFML'] + 0

    # 天降彩珠
    sql_tjcz = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='CHILDLOG_LOG_SKYBOOK_ADD_TIMES_23' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_tjcz)

    data_tjcz = curs.fetchall()
    columns_ = ['u64_1', 'count']

    TJCZ_ = pd.DataFrame(data_tjcz, columns=columns_)
    TJCZ_ = TJCZ_[TJCZ_['u64_1'].isin(final_data['player_id'].tolist())]
    TJCZ_.rename(columns={'u64_1': 'player_id', 'count': 'TJCZCOUNT'}, inplace=True)
    final_data = final_data.join(TJCZ_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyTJCZ'] = final_data['TJCZCOUNT'] > final_data['TJCZCOUNT'].mean(0)
    # final_data['dyTJCZ'] = final_data['dyTJCZ'] + 0
    # final_data['isTJCZ'] = final_data['TJCZCOUNT'] > 0
    # final_data['isTJCZ'] = final_data['isTJCZ'] + 0

    # 追捕邪魔
    sql_zbxm = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='CHILDLOG_LOG_SKYBOOK_ADD_TIMES_23' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_zbxm)

    data_zbxm = curs.fetchall()
    columns_ = ['u64_1', 'count']

    ZBXM_ = pd.DataFrame(data_zbxm, columns=columns_)

    ZBXM_ = ZBXM_[ZBXM_['u64_1'].isin(final_data['player_id'].tolist())]
    ZBXM_.rename(columns={'u64_1': 'player_id', 'count': 'ZBXMCOUNT'}, inplace=True)
    final_data = final_data.join(ZBXM_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyZBXM'] = final_data['ZBXMCOUNT'] > final_data['ZBXMCOUNT'].mean(0)
    # final_data['dyZBXM'] = final_data['dyZBXM'] + 0
    # final_data['isZBXM'] = final_data['ZBXMCOUNT'] > 0
    # final_data['isZBXM'] = final_data['isZBXM'] + 0

    # 怪物围剿
    sql_gwwj = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='CHILDLOG_LOG_SKYBOOK_ADD_TIMES_73' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_gwwj)

    data_gwwj = curs.fetchall()
    columns_ = ['u64_1', 'count']

    GWWJ_ = pd.DataFrame(data_gwwj, columns=columns_)

    GWWJ_ = GWWJ_[GWWJ_['u64_1'].isin(final_data['player_id'].tolist())]
    GWWJ_.rename(columns={'u64_1': 'player_id', 'count': 'GWWJCOUNT'}, inplace=True)
    final_data = final_data.join(GWWJ_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyGWWJ'] = final_data['GWWJCOUNT'] > final_data['GWWJCOUNT'].mean(0)
    # final_data['dyGWWJ'] = final_data['dyGWWJ'] + 0
    # final_data['isGWWJ'] = final_data['GWWJCOUNT'] > 0
    # final_data['isGWWJ'] = final_data['isGWWJ'] + 0

    # 超度游魂
    sql_cdyh = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='CHILDLOG_LOG_SKYBOOK_ADD_TIMES_69' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_cdyh)

    data_cdyh = curs.fetchall()
    columns_ = ['u64_1', 'count']

    CDYH_ = pd.DataFrame(data_cdyh, columns=columns_)

    CDYH_ = CDYH_[CDYH_['u64_1'].isin(final_data['player_id'].tolist())]
    CDYH_.rename(columns={'u64_1': 'player_id', 'count': 'CDYHCOUNT'}, inplace=True)
    final_data = final_data.join(CDYH_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyCDYH'] = final_data['CDYHCOUNT'] > final_data['CDYHCOUNT'].mean(0)
    # final_data['dyCDYH'] = final_data['dyCDYH'] + 0
    # final_data['isCDYH'] = final_data['CDYHCOUNT'] > 0
    # final_data['isCDYH'] = final_data['isCDYH'] + 0

    # 秘宝蒙尘
    sql_mbmc = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='CHILDLOG_LOG_SKYBOOK_ADD_TIMES_5' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_mbmc)

    data_mbmc = curs.fetchall()
    columns_ = ['u64_1', 'count']

    MBMC_ = pd.DataFrame(data_mbmc, columns=columns_)
    MBMC_ = MBMC_[MBMC_['u64_1'].isin(final_data['player_id'].tolist())]
    MBMC_.rename(columns={'u64_1': 'player_id', 'count': 'MBMCCOUNT'}, inplace=True)
    final_data = final_data.join(MBMC_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyMBMC'] = final_data['MBMCCOUNT'] > final_data['MBMCCOUNT'].mean(0)
    # final_data['dyMBMC'] = final_data['dyMBMC'] + 0
    # final_data['isMBMC'] = final_data['MBMCCOUNT'] > 0
    # final_data['isMBMC'] = final_data['isMBMC'] + 0

    # 法宝修炼
    sql_fbxl = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='LOG_FABAO_REFINE' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_fbxl)

    data_fbxl = curs.fetchall()
    columns_ = ['u64_1', 'count']

    FBXL_ = pd.DataFrame(data_fbxl, columns=columns_)
    FBXL_ = FBXL_[FBXL_['u64_1'].isin(final_data['player_id'].tolist())]
    FBXL_.rename(columns={'u64_1': 'player_id', 'count': 'FBXLCOUNT'}, inplace=True)
    final_data = final_data.join(FBXL_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyFBXL'] = final_data['FBXLCOUNT'] > final_data['FBXLCOUNT'].mean(0)
    # final_data['dyFBXL'] = final_data['dyFBXL'] + 0
    # final_data['isFBXL'] = final_data['FBXLCOUNT'] > 0
    # final_data['isFBXL'] = final_data['isFBXL'] + 0

    # 法宝融合
    sql_fbrh = f"select u64_1,count(*) from game_log_pss_final glpf where  server ='{serverid}' and wash_date between '{startdate}' and '{enddate}' and log_type_name ='LOG_FABAO_FUSION' group by u64_1  "

    pg_host = PG_HOST
    pg_port = PG_PORT
    pg_user = PG_USER
    pg_password = PG_PASSWORD
    pg_database = PG_DATABASE
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    curs = conn.cursor()
    curs.execute(sql_fbrh)

    data_fbrh = curs.fetchall()
    columns_ = ['u64_1', 'count']

    FBRH_ = pd.DataFrame(data_fbrh, columns=columns_)
    FBRH_ = FBRH_[FBRH_['u64_1'].isin(final_data['player_id'].tolist())]
    FBRH_.rename(columns={'u64_1': 'player_id', 'count': 'FBRHCOUNT'}, inplace=True)
    final_data = final_data.join(FBRH_.set_index('player_id'), on='player_id', how='left')
    # final_data['dyFBRH'] = final_data['FBRHCOUNT'] > final_data['FBRHCOUNT'].mean(0)
    # final_data['dyFBRH'] = final_data['dyFBRH'] + 0
    # final_data['isFBRH'] = final_data['FBRHCOUNT'] > 0
    # final_data['isFBRH'] = final_data['isFBRH'] + 0

    final_data.fillna(0, inplace=True)
    final_data_ = (final_data - final_data.min()) / (final_data.max() - final_data.min())

    final_data_.fillna(0, inplace=True)

    final_data = final_data_.drop(
        ['player_id', 'banggong', '1login', 'tlogin'], 1)
    final_data.rename(columns={'tsave': 'SAVE'}, inplace=True)

    df = final_data

    # 提出所有feature列
    feature_col_list = df.columns.tolist()
    # print(feature_col_list)
    del feature_col_list[0]

    train, test = train_test_split(df, test_size=DATASPLITRATE, random_state=111)

    X_train = train[feature_col_list]
    y_train = train['SAVE']

    X_test = test[feature_col_list]
    y_test = test['SAVE']

    clf = tree.DecisionTreeClassifier(max_depth=MAXDEPTH)
    # clf = tree.DecisionTreeClassifier(max_depth=MAXDEPTH,min_impurity_decrease=MINIMPURITYDECRESASE)

    clf = clf.fit(X_train, y_train)
    pred = clf.predict(X_test)

    # print(classification_report(y_test, pred))
    # print('------------')
    # print(metrics.f1_score(y_test, pred))

    # for feat, importance in zip(feature_col_list, clf.feature_importances_):
    #     if importance > 0:
    #         print('feature: {f}, importance: {i}'.format(f=feat, i=importance))

    fig, axes = plt.subplots(figsize=(15, 15))

    # tree.plot_tree(clf, feature_names=feature_col_list)

    # plt.show()

    X, y = X_train, y_train

    # print('--------------------')
    # print('logistic regression for feature importance')
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
        # print('Feature:', feature_col_list[int(i)], 'Score: %.2f' % (v * 100))
        LRI.append([feature_col_list[int(i)], v])
    # plot feature importance
    # plt.title('logistic regression for feature importance')
    # pyplot.bar([x for x in range(len(importance))], importance)
    # pyplot.show()

    LRI = pd.DataFrame(LRI)
    # (LRI[LRI[0]=='Forge_time'][1].values))
    final_result.append(float(LRI[LRI[0] == 'Forge_time'][1].values))
    # internal_chars = df.columns.tolist()
    # corrmat = df[internal_chars].corr()
    # f, ax = plt.subplots(figsize=(20, 10))
    # plt.xticks(rotation='0')
    # sns.heatmap(corrmat, square=False, linewidths=.4, annot=True)

print(final_result)
plt.plot(range(len(final_result)), final_result)
plt.show()

