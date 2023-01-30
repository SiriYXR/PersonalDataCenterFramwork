import time
import os
import re
from datetime import datetime,timedelta
import pandas as pd 
from sqlalchemy import create_engine,types
import pymysql

db_user="your_user_name" # 用于连接数据库的用户名
db_passwd="your_password" # 用于连接数据库的用户密码
db_dbname="your_database_name" # 数据库中待连接的库名
db_hostname="your_hostname" # 数据库主机地址

def siridw_data_backup():
    bt=time.time()
    print_with_time("开始数据备份")

    # 想要备份的数据表
    table_name=["dwd_book_read_record",
                "dim_notiondb_book_list_info",
                "dim_notiondb_video_list_info",
                # ...
                ]

    # 生产环境 本地存储备份数据的文件夹
    backup_root_dir="/your_path/siridw_backup" #你的数据备份文件夹

    # 开发环境
    # backup_root_dir="./data"

    backup_dir=os.path.join(backup_root_dir,datetime.now().strftime("%Y-%m-%d"))

    os.makedirs(backup_dir, exist_ok=True)

    for name in table_name:
        dump_table(name,backup_dir)
    
    clear_old_data(backup_root_dir,30)
    print_with_time("数据备份完成，用时："+str(time.time()-bt))

def dump_table(table_name,backup_dir):
    bt=time.time()

    con_engine = create_engine('mysql+pymysql://'+db_user+':'+db_passwd+'@'+db_hostname+':3306/'+db_dbname+'?charset=utf8')

    sql = "select * from "+table_name

    df=pd.read_sql(sql,con=con_engine)

    df.to_csv(os.path.join(backup_dir,table_name+".csv"),index=False)

    print_with_time("备份完成："+table_name+"\t\t备份数据量："+str(len(df))+"\t\t用时："+str(time.time()-bt))

def clear_old_data(backup_root_dir,life_time):
    dirs=os.listdir(backup_root_dir)

    for d in dirs:
        if(re.match('\d{4}-\d{2}-\d{2}',d)!=None and (datetime.now()-datetime.strptime(d,"%Y-%m-%d")).days>life_time):
            res=os.popen("rm -rf "+os.path.join(backup_root_dir,d)).read()
            print_with_time("清除过期备份："+os.path.join(backup_root_dir,d))
            
def print_with_time(msg):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"\t"+str(msg))

if __name__ == "__main__" :
    siridw_data_backup()