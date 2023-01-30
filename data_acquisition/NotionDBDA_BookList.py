import requests
import json
import time
import operator
from datetime import datetime,timedelta
import pandas as pd 
from sqlalchemy import create_engine,types
import pymysql

from NotionAPI import *

token="your_notion_robot_token" # Notion机器人token
NotionAPI="https://api.notion.com/v1/"
Notion_Version="2021-08-16"

book_list_dbid="your_notion_datatable_id" # 待采集的Notion数据表id

db_user="your_user_name" # 用于连接数据库的用户名
db_passwd="your_password" # 用于连接数据库的用户密码
db_dbname="your_database_name" # 数据库中待连接的库名
db_hostname="your_hostname" # 数据库主机地址

def GetBookListAllRecord():

    # 从Notion获取书单所有记录
    results=NotionGetAllRecord(book_list_dbid,NotionAPI,token,Notion_Version)

    # 解析字段
    object_id=[]
    book_name=[]
    auther_name=[]
    auther_country=[]
    book_type=[]
    book_price=[]
    book_patform=[]
    read_datetime_start=[]
    read_datetime_end=[]
    book_page_readed=[]
    book_page_all=[]
    status=[]
    for i in results:

        object_id.append(i["id"])

        book_name.append(NotionPropertyParse_Title(i["properties"]["书名"]))

        auther_name.append(NotionPropertyParse_Text(i["properties"]["作者"]))

        auther_country.append(NotionPropertyParse_Select(i["properties"]["国家"]))

        book_type.append(",".join(NotionPropertyParse_Multi_Select(i["properties"]["类型"])))

        book_price.append(NotionPropertyParse_Number(i["properties"]["价格"]))

        book_patform.append(",".join(NotionPropertyParse_Multi_Select(i["properties"]["平台"])))

        read_datetime_start.append(NotionPropertyParse_Date_Start(i["properties"]["阅读日期"]))

        read_datetime_end.append(NotionPropertyParse_Date_End(i["properties"]["阅读日期"]))
        
        book_page_readed.append(NotionPropertyParse_Number(i["properties"]["已阅读量"]))

        book_page_all.append(NotionPropertyParse_Number(i["properties"]["总量"]))

        status.append(NotionPropertyParse_Select(i["properties"]["状态"]))

    # 创建pandas数据表
    df=pd.DataFrame({"id":object_id,"book_name":book_name,"auther_name":auther_name,"auther_country":auther_country,"book_type":book_type,"book_price":book_price,"book_patform":book_patform,"read_datetime_start":read_datetime_start,"read_datetime_end":read_datetime_end,"book_page_readed":book_page_readed,"book_page_all":book_page_all,"status":status})

    # 连接数据库
    con_engine = create_engine('mysql+pymysql://'+db_user+':'+db_passwd+'@'+db_hostname+':3306/'+db_dbname+'?charset=utf8')

    # 定义数据表字段类型约束
    dtype={"id":types.String(length=255),
            "book_name":types.String(length=255),
            "auther_name":types.String(length=255),
            "auther_country":types.String(length=255),
            "book_type":types.String(length=255),
            "book_price":types.Float(),
            "book_patform":types.String(length=255),
            "read_datetime_start":types.DateTime(),
            "read_datetime_end":types.DateTime(),
            "book_page_readed":types.Float(),
            "book_page_all":types.Float(),
            "status":types.String(length=255)
    }

    # 写入数据到MySQL
    df.to_sql('dim_notiondb_book_list_info', con_engine, dtype=dtype, if_exists='replace', index = False)


if __name__ == "__main__":
    bt=time.time()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"开始采集dim_notiondb_book_list_info")

    GetBookListAllRecord()

    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"dim_notiondb_book_list_info采集结束",time.time()-bt)