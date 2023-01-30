import json
import time
from datetime import datetime,timedelta
import pandas as pd 
from sqlalchemy import create_engine,types
import pymysql

from SteamWebAPI import *

tocken="your_steam_tocken" # 你的steam_tocken
steamid="your_steam_id" # 你的steam_id

db_user="your_user_name" # 用于连接数据库的用户名
db_passwd="your_password" # 用于连接数据库的用户密码
db_dbname="your_database_name" # 数据库中待连接的库名
db_hostname="your_hostname" # 数据库主机地址

def SteamDA_OwnedGames(tocken,steamid):

    results=Steam_GetOwnedGames(tocken,steamid)

    # print(results["response"]["games"][0])

    appid=[]
    game_name=[]
    img_icon_url=[]
    playtime_forever=[]
    playtime_windows_forever=[]
    playtime_mac_forever=[]
    playtime_linux_forever=[]
    rtime_last_played=[]

    for i in results["response"]["games"]:
        appid.append(i["appid"])
        game_name.append(i["name"])
        img_icon_url.append(i["img_icon_url"])
        playtime_forever.append(i["playtime_forever"])
        playtime_windows_forever.append(i["playtime_windows_forever"])
        playtime_mac_forever.append(i["playtime_mac_forever"])
        playtime_linux_forever.append(i["playtime_linux_forever"])
        rtime_last_played.append(i["rtime_last_played"])

    df=pd.DataFrame({"appid":appid,"game_name":game_name,"img_icon_url":img_icon_url,"playtime_forever":playtime_forever,"playtime_windows_forever":playtime_windows_forever,"playtime_mac_forever":playtime_mac_forever,"playtime_linux_forever":playtime_linux_forever,"rtime_last_played":rtime_last_played})

    # print(df)

    con_engine = create_engine('mysql+pymysql://'+db_user+':'+db_passwd+'@'+db_hostname+':3306/'+db_dbname+'?charset=utf8')

    dtype={"appid":types.String(length=255),
            "game_name":types.String(length=255),
            "img_icon_url":types.String(length=255),
            "playtime_forever":types.Integer(),
            "playtime_windows_forever":types.Integer(),
            "playtime_mac_forever":types.Integer(),
            "playtime_linux_forever":types.Integer(),
            "rtime_last_played":types.Integer()
    }

    df.to_sql('dim_steam_owned_game', con_engine, dtype=dtype, if_exists='replace', index = False)

def SteamDA_GamePlayedRecord():

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
        INSERT INTO dwd_steam_game_played_record 
        SELECT 
                NULL id,
                t1.appid,
                t1.game_name,
                COALESCE(t1.playtime_forever - t2.playtime,0) playtime,
                COALESCE(t1.playtime_windows_forever - t2.playtime_windows,0) playtime_windows,
                COALESCE(t1.playtime_mac_forever - t2.playtime_mac,0) playtime_mac,
                COALESCE(t1.playtime_linux_forever - t2.playtime_linux,0) playtime_linux,
                t1.rtime_last_played,
                NOW() create_time,
                NOW() update_time 
        FROM (
        SELECT 
                    appid,
                    game_name,
                    playtime_forever,
                    playtime_windows_forever,
                    playtime_mac_forever,
                    playtime_linux_forever,
                    rtime_last_played
        FROM dim_steam_owned_game
        ) t1
        LEFT JOIN
        (
        SELECT 
                    appid,
                    SUM(playtime) playtime,
                    SUM(playtime_windows) playtime_windows,
                    SUM(playtime_mac) playtime_mac,
                    SUM(playtime_linux) playtime_linux,
                    MAX(rtime_last_played) rtime_last_played
        FROM dwd_steam_game_played_record
        GROUP BY appid,game_name
        )t2
        ON t1.appid=t2.appid
        WHERE t2.rtime_last_played IS NULL OR t1.rtime_last_played !=t2.rtime_last_played
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
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"开始采集Steam游戏数据")

    SteamDA_OwnedGames(tocken,steamid)
    SteamDA_GamePlayedRecord()

    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"Steam游戏数据采集结束",time.time()-bt)