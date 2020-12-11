import numpy as np
import pandas as pd
import psycopg2
import datetime



time_start = '2020-10-07'
time_end = '2020-11-04'
server_id = 'glb00017'
psn_lv_30 = pd.read_csv('D:/data/A10_30_offline.csv')
BH_ = pd.read_csv('D:/data/A10BH_1.csv')

dead_time = datetime.datetime.strptime(str(time_end), '%Y-%m-%d')
interval_time = datetime.timedelta(days=7)



sql_list = ['LOG_GUILD_CREATE_CONSUME', 'LOG_GUILD_CREATE', 'LOG_GUILD_LEV_UP', 'LOG_TYPE_GUILD_ADD_WORSHIP',
            'LOG_TYPE_GUILD_SUB_WORSHIP', 'LOG_TYPE_GUILD_GET_CHEST', 'LOG_TYPE_GUILD_ACCUSE',
            'LOG_TYPE_GUILD_CHANGE_NAME', 'LOG_TYPE_GUILD_FINISH_STONE_TASK', 'LOG_TYPE_GUILD_HELP_FINISH_STONE_TASK',
            'LOG_TYPE_GUILD_DONATE', 'LOG_TYPE_GUILD_DICE_REWARD', 'LOG_AUCTION_BID_GUILD_ITEM',
            'LOG_AUCTION_BID_GUILD_ITEM_FAILED', 'LOG_AUCTION_BID_GUILD_ITEM_SUCCESS',
            'LOG_AUCTION_BID_GUILD_ITEM_FAILED_MAIL', 'LOG_AUCTION_BID_GUILD_ITEM_SUCCESS_MAIL',
            'LOG_AUCTION_GUILD_SELL_ITEM', 'LOG_AUCTION_GUILD_BID_ITEM_SUCCESS', 'LOG_AUCTION_GUILD_BID_ITEM_BACK',
            'LOG_AUCTION_GUILD_ITEM_DELAY', 'LOG_TYPE_GUILD_PVP_GUILD_AWARD', 'LOG_GUILD_ALLOT_BONUS_CREATE',
            'LOG_GUILD_AUTO_LEAVE', 'LOG_GUILD_AUTO_DISBAND', 'LOG_GUILD_WATER_DO', 'LOG_GUILD_WATER_FRUIT_NEW',
            'LOG_GUILD_WATER_FRUIT_DO', 'LOG_GUILD_WATER_ACT_END', 'LOG_GUILD_POSITION_AWARD', 'LOG_GUILD_DONATION',
            'LOG_ACT_HAOBANGZHU_VOTE', 'LOG_ACT_HAOBANGZHU_ITEM', 'LOG_ACT_HAOBANGZHU_PAY', 'LOG_GUILD_BUILD_SUB_ITEM',
            'LOG_GUILD_PAIR_PUBLISH', 'LOG_GUILD_PAIR_BACKOUT', 'LOG_GUILD_PAIR_MATCH', 'LOG_GUILD_PAIR_SHARE',
            'LOG_GUILD_PAIR_LEAVE', 'LOG_GUILD_WATER_DO_NEW', 'LOG_CITY_WAR_UPDATE_HOLD_TIME',
            'LOG_CITY_WAR_DELETE_HOLD_TIME', 'LOG_GUILD_GM_TRY_JOIN_GUILD', 'LOG_GUILD_SEND_ITEMS',
            'LOG_GUILD_RECEIVE_ITEMS', 'LOG_TYPE_GUILD_WATCH_STAR', 'LOG_CITY_WAR_AWARD_PLY_SEND_AWARD',
            'LOG_CITY_WAR_END_GUILD_HOLD_TIME', 'LOG_CITY_WAR_WIN_GUILD', 'LOG_CITY_WAR_GUILD',
            'LOG_CITY_WAR_AWARD_SEND_AWARD', 'LOG_TYPE_GUILD_OPEN_TREE_ANSWER_QUESTION',
            'LOG_TYPE_GUILD_OPEN_TREE_ACTIVITY', 'LOG_TYPE_GUILD_OPEN_TREE_DRINK', 'LOG_GUILD_TIANZHU_ITEM_USE',
            'LOG_GUILD_TIANZHU_SUCC', 'LOG_GUILD_TIANZHU_REPAY_MAIL', 'LOG_GUILD_TIANZHU_PUNISH',
            'LOG_GUILD_TIANZHU_AWARD', 'LOG_GUILD_TIANZHU_RELIFE', 'LOG_PROTECT_TREE_SETTLE_AWARD',
            'LOG_PROTECT_TREE_ADD_BUFF', 'LOG_PROTECT_TREE_WAVE_STRENGTH', 'LOG_PROTECT_TREE_WAVE_COSTTIME',
            'LOG_PROTECT_TREE_OPEN', 'LOG_TYPE_GUILD_TRIAL_JIHUO', 'LOG_TYPE_GUILD_TRIAL_OPEN',
            'LOG_TYPE_GUILD_TRIAL_JOIN', 'LOG_TYPE_GUILD_TRIAL_RESULT', 'LOG_TYPE_GUILD_TRIAL_PLAYER_RESULT',
            'LOG_TYPE_GUILD_TRIAL_JIHUO_SUCCESS', 'LOG_TYPE_GUILD_TRIAL_OPEN_SUCCESS', 'LOG_ADD_BANGGONG',
            'LOG_TYPE_GUILD_BOSS_REWARD']
sql_1 = """ 
select  u64_1  ,count(*) from game_log_pss_final glpf where log_type_name = """
#sql_2 = """ and  "server" ='glb00011' and wash_date between '2020-09-18' and '2020-10-14' group by u64_1 """

sql_2_1=""" and  "server" = """
sql_2_2="""and wash_date between"""
sql_2_3="""and"""
sql_2_4="""group by u64_1 """






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

# print(final_data)
print('一共有', len(sql_list), '个列')
for n in sql_list:
    sql = sql_1 + '\'' + n + '\'' + sql_2_1 + '\'' +server_id+ '\''+ sql_2_2 + '\''+time_start + '\'' + sql_2_3 + '\''  +time_end +'\''  + sql_2_4
    # print(sql)

    # sqlcolumn
    # column_sql_ = "select COLUMN_NAME from information_schema.COLUMNS where table_name = 'game_log_pss_final'"

    pg_host = "192.168.2.154"
    pg_port = "5432"
    pg_user = "cyread"
    pg_password = "oS8lckyVT9q9q0qr"
    pg_database = "db_ana_pss_glb"
    pg_table = ""

    conn_string = "host=" + pg_host + " port=" + pg_port + " dbname=" + pg_database + " user=" + pg_user + " password=" + pg_password
    conn = psycopg2.connect(conn_string)

    columns_ = ['u64_1', str(n)]

    curs = conn.cursor()
    curs.execute(sql)

    data_ = curs.fetchall()
    data_ = pd.DataFrame(data_, columns=columns_)
    # data_ = data_[['u64_1','wash_date']]
    # data_.drop_duplicates(subset='u64_1',keep='first',inplace=True)
    # data_ = data_.reset_index()
    data_ = data_[data_['u64_1'].isin(final_data['player_id'].tolist())]
    conn.commit()
    curs.close()
    conn.close()
    data_.rename(columns={'u64_1': 'player_id'}, inplace=True)
    # data_.rename(columns={'count': n}, inplace=True)

    final_data = final_data.join(data_.set_index('player_id'), on='player_id', how='left')
    # print(data_)

    # final_data['test']= final_data[n] > 0
    # final_data['test']= final_data[n] + 0
    # is_ACT = final_data['test']
    # final_data.insert(is_ACT)
    #
    # final_data['test'] =final_data[n]>  final_data[n].mean(0)
    # final_data['test'] = final_data['test'] + 0
    # dy_ACT = final_data['test']
    # final_data.insert(dy_ACT)

    print('-------->')
    print('完成','%.2f' %(sql_list.index(n)*100/len(sql_list)),'%','还剩','%.2f' %((len(sql_list) - sql_list.index(n))*100/len(sql_list)),'%')


print('--')

# final_data = final_data.drop(['banggong'], 1)
final_data.fillna(0, inplace=True)
print(final_data.columns)
print(final_data.head)
final_data.to_csv('D:/data/partyfileS10.csv')
