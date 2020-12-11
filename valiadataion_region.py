# -*- coding: utf-8 -*-
import pandas as pd
import psycopg2
import numpy as np
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif']=['SimHei']

plt.rcParams['axes.unicode_minus'] = False
import datetime



#参数设置
time_start = '2020-09-15'
time_end = '2020-10-14'
server_id = 'glb00011'

#固定参数

# select country ,wash_date ,count(1) from player_create_all where server ='glb00011' group by country ,wash_date order by wash_date
sql1_1 = """ select country ,wash_date ,count(1) from player_create_all where server = """
sql1_1_1="""and wash_date between"""
sql1_1_2="""and"""
sql1_1_3=""" group by country ,wash_date order by wash_date   """

sql1 = sql1_1 + '\'' +server_id+ '\''+ sql1_1_1 + '\''+time_start + '\'' + sql1_1_2 + '\''  +time_end +'\''  + sql1_1_3
print(sql1)

import MySQLdb

zc_table=[]

conn= MySQLdb.connect(
        host='xxx',
        port = xxx,
        user='xxx',
        passwd='xxx',
        db ='xxx',
        )
xxx


#通过获取到的数据库连接conn下的cursor()方法来创建游标。
cur = conn.cursor()
for n in range(len(n17)):
    #创建数据表,通过游标cur 操作execute()方法可以写入纯sql语句。通过execute()方法中写如sql语句来对数据进行操作
    sql_1= """SELECT ip from db_ana_t11.plat_login_new pln where acct =   """
    sql_2=n17.iloc[n][0]
    sql_ = sql_1 + '\'' +sql_2+ '\''
    #print(sql_)
    cur.execute(sql_)
    rows_ = cur.fetchall()
    print(rows_)
    zc_table.append(rows_)
#cur.close()
cur.close()

#conn.commit()方法在提交事物，在向数据库插入一条数据时必须要有这个方法，否则数据不会被真正的插入。
conn.commit()

#conn.close()关闭数据库连接
conn.close()