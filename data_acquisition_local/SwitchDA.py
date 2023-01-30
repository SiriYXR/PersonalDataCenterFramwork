import json
import time
from datetime import datetime,timedelta
import pandas as pd 
from sqlalchemy import create_engine,types
import pymysql

from SwitchWebAPI import *

client_id = "5c38e31cd085304b" # 你生成session_token时使用的client_id
ua = 'com.nintendo.znej/1.13.0 (Android/7.1.2)'
session_token="your_session_token" #你的session_token

db_user="your_user_name" # 用于连接数据库的用户名
db_passwd="your_password" # 用于连接数据库的用户密码
db_dbname="your_database_name" # 数据库中待连接的库名
db_hostname="your_hostname" # 数据库主机地址

def SwitchDA_GamePlayHistory(client_id,session_token,ua):
    results=NS_GetPlayHistory(NS_GetAccessToken(client_id,session_token),ua)

    titleId=[]
    titleName=[]
    deviceType=[]
    imageUrl=[]
    lastUpdatedAt=[]
    firstPlayedAt=[]
    lastPlayedAt=[]
    totalPlayedDays=[]
    totalPlayedMinutes=[]

    for i in results["playHistories"]:
        titleId.append(i["titleId"])
        titleName.append(i["titleName"])
        deviceType.append(i["deviceType"])
        imageUrl.append(i["imageUrl"])
        lastUpdatedAt.append(i["lastUpdatedAt"])
        firstPlayedAt.append(i["firstPlayedAt"])
        lastPlayedAt.append(i["lastPlayedAt"])
        totalPlayedDays.append(i["totalPlayedDays"])
        totalPlayedMinutes.append(i["totalPlayedMinutes"])
    
    df=pd.DataFrame({"titleId":titleId,"titleName":titleName,"deviceType":deviceType,"imageUrl":imageUrl,"lastUpdatedAt":lastUpdatedAt,"firstPlayedAt":firstPlayedAt,"lastPlayedAt":lastPlayedAt,"totalPlayedDays":totalPlayedDays,"totalPlayedMinutes":totalPlayedMinutes})

    UTC9to8=lambda x: (datetime.strptime(x,"%Y-%m-%dT%H:%M:%S+09:00")-timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    df["lastUpdatedAt"]=df["lastUpdatedAt"].map(UTC9to8)
    df["firstPlayedAt"]=df["firstPlayedAt"].map(UTC9to8)
    df["lastPlayedAt"]=df["lastPlayedAt"].map(UTC9to8)

    # print(df)

    con_engine = create_engine('mysql+pymysql://'+db_user+':'+db_passwd+'@'+db_hostname+':3306/'+db_dbname+'?charset=utf8')

    dtype={"titleId":types.String(length=255),
            "titleName":types.String(length=255),
            "deviceType":types.String(length=255),
            "imageUrl":types.String(length=255),
            "lastUpdatedAt":types.DateTime(),
            "firstPlayedAt":types.DateTime(),
            "lastPlayedAt":types.DateTime(),
            "totalPlayedDays":types.Integer(),
            "totalPlayedMinutes":types.Integer()
    }

    df.to_sql('dim_switch_game_play_history', con_engine, dtype=dtype, if_exists='replace', index = False)


def SwitchDA_GamePlayedRecord():

    db = pymysql.connect(
        host=db_hostname, 
        port=3306,
        user=db_user,    #在这里输入用户名
        password=db_passwd,     #在这里输入密码
        charset='utf8mb4',
        database=db_dbname    #指定操作的数据库
        )

    cursor = db.cursor() #创建游标对象

    try:
        sql="""
        INSERT INTO dwd_switch_game_played_record 
        SELECT 
                NULL id,
                t1.titleId ,
                t1.titleName ,
                t1.lastPlayedAt,
                COALESCE(t1.totalPlayedMinutes  - t2.play_time,0) play_time,
                NOW() create_time,
                NOW() update_time 
        FROM (
        SELECT 
                    titleId ,
                    titleName ,
                    lastPlayedAt ,
                    totalPlayedMinutes
        FROM dim_switch_game_play_history
        ) t1
        LEFT JOIN
        (
        SELECT 
                    title_id ,
                    SUM(play_time) play_time,
                    MAX(last_played_at) last_played_at
        FROM dwd_switch_game_played_record
        GROUP BY title_id
        )t2
        ON t1.titleId=t2.title_id
        WHERE t2.last_played_at IS NULL OR t1.lastPlayedAt !=t2.last_played_at
        """
        # print(sql)
        cursor.execute(sql)
        db.commit()

    except Exception as e:
        print(e)
        db.rollback()  #回滚事务

    finally:
        cursor.close() 
        db.close()  #关闭数据库连接


if __name__ == "__main__":

    bt=time.time()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"开始采集Switch游戏数据")

    SwitchDA_GamePlayHistory(client_id,session_token,ua)
    SwitchDA_GamePlayedRecord()

    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"Switch游戏数据采集结束",time.time()-bt)