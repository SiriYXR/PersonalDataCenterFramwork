import re
import os
from datetime import datetime,timedelta
from utils import *
import math
from NotionAPI import *

from sqlalchemy import create_engine,types
import pymysql

class ReadRecordAction():

    def __init__(self):
        self.token="your_notion_robot_token" # 你的Notion机器人token
        self.NotionAPI="https://api.notion.com/v1/"
        self.Notion_Version="2021-08-16"

        self.book_list_dbid="your_book_list_dbid" # 你的Notion书单数据库id

        self.db_user="your_user_name" # 用于连接数据库的用户名
        self.db_passwd="your_password" # 用于连接数据库的用户密码
        self.db_dbname="your_database_name" # 数据库中待连接的库名
        self.db_hostname="your_hostname" # 数据库主机地址
        
        # 生产环境
        self.book_read_record_table="dwd_book_read_record"
        self.py_path="/your_path/miniconda3/bin/python" # 你的python解释器路径
        self.script_path="/your_path/data_acquisition/" # 你的数据采集模块路径
        
    def doAction(self,args):

        if(len(args)==2 and args[0]=="帮助" and args[1]=="阅读记录"):
            return """帮助文档：阅读记录
阅读记录
- 查询正在阅读的记录

阅读开始 [书名] 开始：[开始页码]
- 阅读开始

阅读结束 开始：[开始页码] 结束：结束页码|已读完 时长：[阅读时长]
- 结束并完成一次阅读

阅读取消
- 取消一个未完成的阅读
"""

        # 阅读记录
        if(len(args)==1 and args[0]=="阅读记录"):
            db = pymysql.connect(
                host=self.db_hostname, 
                port=3306,
                user=self.db_user,    
                password=self.db_passwd,     
                charset='utf8mb4',
                database=self.db_dbname 
                )

            cursor = db.cursor() #创建游标对象

            try:
                sql = "SELECT book_name,datetime_start FROM dwd_book_read_record WHERE status='reading'"
                print(sql)
                cursor.execute(sql)
                records=cursor.fetchall()

                if(len(records)==0):
                    return "当前无正在阅读的记录"

                return "正在阅读："+records[0][0]+\
                        "\n开始时间："+records[0][1].strftime("%Y-%m-%d %H:%M:%S")+\
                        "\n已读"+str(int((datetime.now()-records[0][1]).total_seconds()/60))+"分钟"


            except Exception as e:
                print(e)
                db.rollback()  #回滚事务
            finally:
                cursor.close() 
                db.close()  #关闭数据库连接

        # 阅读取消
        if(len(args)==1 and (args[0]=="阅读取消" or args[0]=="取消阅读")):
            db = pymysql.connect(
                host=self.db_hostname, 
                port=3306,
                user=self.db_user,    
                password=self.db_passwd,     
                charset='utf8mb4',
                database=self.db_dbname     
                )

            cursor = db.cursor() #创建游标对象

            try:
                sql = "SELECT id,book_name,datetime_start FROM "+self.book_read_record_table+" WHERE status='reading'"
                print(sql)
                cursor.execute(sql)
                records=cursor.fetchall()

                if(len(records)==0):
                    return "当前无正在阅读的记录"

                sql = "UPDATE "+self.book_read_record_table+" SET status='cancel' WHERE id=%s"%(records[0][0])
                print(sql)
                cursor.execute(sql)
                db.commit()
                return "已取消阅读："+records[0][1]+" "+records[0][2].strftime("%Y-%m-%d %H:%M:%S")

            except Exception as e:
                print(e)
                db.rollback()  #回滚事务

            finally:
                cursor.close() 
                db.close()  #关闭数据库连接

        # 阅读开始
        if(len(args)>1 and (args[0]=="阅读开始" or args[0]=="开始阅读")):

            name_s=[]
            page_start=None
            for i in args[1:]:
                if(re.match("开始[:：](-?\d+)(\.\d+)?$",i)!=None):
                    page_start=float(re.match("开始[:：](-?\d+)(\.\d+)?$",i).group(1))
                else:
                    name_s.append(i)
            book_name=" ".join(name_s)

            db = pymysql.connect(
                host=self.db_hostname, 
                port=3306,
                user=self.db_user,    
                password=self.db_passwd,     
                charset='utf8mb4',
                database=self.db_dbname     
                )

            cursor = db.cursor() #创建游标对象

            try:
                # 检查是否有正在阅读的记录
                sql = "SELECT book_name,datetime_start FROM "+self.book_read_record_table+" WHERE status='reading'"
                print(sql)
                cursor.execute(sql)
                records=cursor.fetchall()
                print(records)

                if(len(records)>0):
                    return "请先结束阅读 "+records[0][0]+" "+records[0][1].strftime("%Y-%m-%d %H:%M:%S")
                  
                records=self.NotionGetBookListByName(book_name)
                
                # 检查该书是否存在于书单中
                if(len(records)==0):
                    return  book_name+" 未录入书单"

                if(len(records)>1):
                    return  book_name+" 存在重复书单记录"

                page_all=NotionPropertyParse_Number(records[0]["properties"]["总量"])
                if page_all==None or page_all<=0:
                    return  book_name+" 页码总量未设置"

                sql = """
                SELECT 
                        MAX(page_end) AS last_page_end,
                        SUM(read_time) AS read_time,
                        COUNT(DISTINCT DATE_FORMAT(datetime_start ,'%Y-%m-%d')) AS read_days,
                        SUM(read_time)/MAX(page_end) AS page_read_time_avg,
                        SUM(read_time)/COUNT(DISTINCT DATE_FORMAT(datetime_start ,'%Y-%m-%d')) AS day_read_time_avg
                FROM """+self.book_read_record_table+""" 
                WHERE datetime_start >(
                    SELECT COALESCE(MAX(datetime_start),'2000-01-01 00:00:00')
                    FROM	"""+self.book_read_record_table+"""
                    WHERE	is_finish=1 AND book_name='"""+book_name+"""'
                )
                AND book_name = '"""+book_name+"""'
                """
                print(sql)
                cursor.execute(sql)
                records=cursor.fetchall()
                print(records)

                # 获取上一次阅读结束页码
                if page_start==None:
                    page_start=0

                    if len(records)>0 and records[0][0]!=None:
                        page_start=float(records[0][0])

                datetime_start=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                sql = "INSERT INTO "+self.book_read_record_table+"(book_name,page_start,datetime_start,status) VALUES('%s',%s,'%s','reading')"%(book_name,str(page_start),datetime_start)
                print(sql)
                cursor.execute(sql)
                db.commit()
                res="开始阅读 "+book_name+"\n于"+datetime_start+"第"+str(page_start)+"页"

                # 开始阅读分析
                if len(records)==0 or records[0][0]==None:
                    res+="\n这是本书第一次阅读"
                else:
                    read_time_minute=records[0][1]
                    read_time_day=records[0][2]
                    read_percentage=float(records[0][0])/page_all*100
                    read_time_last_minute=(page_all-float(records[0][0]))*float(records[0][3])
                    read_time_last_day=math.ceil(((page_all-float(records[0][0]))*float(records[0][3]))/float(records[0][4]))

                    res+="\n已阅读%s，阅读天数%d天，阅读进度%.2f"%(time_format(read_time_minute),read_time_day,float(read_percentage))+"%"
                    res+="\n读完本书估计仍需%s，预计%d天后读完"%(time_format(read_time_last_minute),read_time_last_day)

                return res

            except Exception as e:
                print(e)
                db.rollback()  #回滚事务
            finally:
                cursor.close() 
                db.close()  #关闭数据库连接

        # 阅读结束
        if(len(args)>=1 and (args[0]=="阅读结束" or args[0]=="结束阅读")):
            
            page_start=None
            page_end=None
            read_time=None
            is_finish=None
            last_args=[]
            for i in args[1:]:
                if(re.match("开始[:：](-?\d+)(\.\d+)?$",i)!=None):
                    try:
                        page_start=float(re.match("开始[:：](-?\d+)(\.\d+)?$",i).group(1))
                    except Exception as e:
                        return "【开始】参数格式错误！"
                elif(re.match("结束[:：](-?\d+)(\.\d+)?$",i)!=None):
                    try:
                        page_end=float(re.match("结束[:：](-?\d+)(\.\d+)?$",i).group(1))
                    except Exception as e:
                        return "【结束】参数格式错误！"
                elif(re.match("时长[:：](-?\d+)$",i)!=None):
                    try:
                        read_time=int(re.match("时长[:：](-?\d+)$",i).group(1))
                    except Exception as e:
                        return "【时长】参数格式错误！"
                elif(re.match("已读完|已看完",i)!=None):
                    is_finish=1
                else:
                    last_args.append(i)
            
            if page_end==None and len(last_args)>0:
                try:
                   page_end= float(" ".join(last_args))
                except Exception as e:
                    page_end=None   

            if is_finish==None and page_end==None:
                return "请置结束页码！"

            db = pymysql.connect(
                host=self.db_hostname, 
                port=3306,
                user=self.db_user,    
                password=self.db_passwd,     
                charset='utf8mb4',
                database=self.db_dbname     
                )

            cursor = db.cursor() #创建游标对象

            try:
                sql = "SELECT id,book_name,page_start,datetime_start FROM "+self.book_read_record_table+" WHERE status='reading'"
                print(sql)
                cursor.execute(sql)
                records=cursor.fetchall()
                
                if(len(records)==0):
                    return "当前无正在阅读的记录！"

                record_id=records[0][0]
                book_name=records[0][1]
                if page_start==None:
                    page_start=records[0][2]
                datetime_start=records[0][3]
                datetime_end=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if read_time==None:
                    read_time=int((datetime.now()-datetime_start).total_seconds()/60)
                if read_time<=0: #阅读时长不得小于等于0
                    read_time=1

                records=self.NotionGetBookListByName(book_name)
                # 检查该书是否存在于书单中
                if(len(records)==0):
                    return  book_name+" 未录入书单！"

                if(len(records)>1):
                    return  book_name+" 存在重复书单记录！"

                page_all=NotionPropertyParse_Number(records[0]["properties"]["总量"])
                if page_all==None or page_all<=0:
                    return  book_name+" 页码总量未设置！"

                if page_end!=None:
                    if page_end<=0:
                        return  book_name+" 结束页码无效！"
                
                    if page_end>=page_all:
                        page_end=page_all
                        is_finish=1
                
                if is_finish!=None:
                    page_end=page_all
                else:
                    is_finish=0
                
                sql = "UPDATE "+self.book_read_record_table+" SET status='end',page_start=%s,page_end=%s,datetime_end='%s',read_time=%s,is_finish=%d WHERE id=%s"%(page_start,page_end,datetime_end,read_time,is_finish,record_id)
                print(sql)
                cursor.execute(sql)
                db.commit()

                start_date=NotionPropertyParse_Date_Start(records[0]["properties"]["阅读日期"])
                if start_date == None:
                    start_date=datetime.now().strftime("%Y-%m-%d")
                
                end_date=NotionPropertyParse_Date_End(records[0]["properties"]["阅读日期"])
                if is_finish==1:
                    end_date=datetime.now().strftime("%Y-%m-%d")

                status="在读"
                if is_finish==1:
                    status="已读"

                properties={
                    "已阅读量":{"number":page_end},
                    "阅读日期":{"date":{"start":start_date,"end":end_date}},
                    "状态":{"select":{"name":status}}
                }
                self.NotionUpdateBookListByID(records[0]['id'],properties)
                
                if(is_finish==0):
                    res= "已结束阅读："+book_name+" "+datetime_end+\
                            "\n阅读页数："+str(page_end-page_start)+\
                            "\n阅读时长："+time_format(read_time)
                    
                    sql = """
                    SELECT 
                            MAX(page_end) AS last_page_end,
                            SUM(read_time) AS read_time,
                            COUNT(DISTINCT DATE_FORMAT(datetime_start ,'%Y-%m-%d')) AS read_days,
                            SUM(read_time)/MAX(page_end) AS page_read_time_avg,
                            SUM(read_time)/COUNT(DISTINCT DATE_FORMAT(datetime_start ,'%Y-%m-%d')) AS day_read_time_avg
                    FROM """+self.book_read_record_table+""" 
                    WHERE datetime_start >(
                        SELECT COALESCE(MAX(datetime_start),'2000-01-01 00:00:00')
                        FROM	"""+self.book_read_record_table+"""
                        WHERE	is_finish=1 AND book_name='"""+book_name+"""'
                    )
                    AND book_name = '"""+book_name+"""'
                    """
                    print(sql)
                    cursor.execute(sql)
                    records=cursor.fetchall()
                    print(records)

                    read_time_minute=records[0][1]
                    read_time_day=records[0][2]
                    read_percentage=float(records[0][0])/page_all*100
                    read_time_last_minute=(page_all-float(records[0][0]))*float(records[0][3])
                    read_time_last_day=math.ceil(((page_all-float(records[0][0]))*float(records[0][3]))/float(records[0][4]))

                    res+="\n已阅读%s，阅读天数%d天，阅读进度%.2f"%(time_format(read_time_minute),read_time_day,float(read_percentage))+"%"
                    res+="\n读完本书估计仍需%s，预计%d天后读完"%(time_format(read_time_last_minute),read_time_last_day)

                else:
                    res= "已读完 "+book_name+" "+datetime_end
                
                # 同步书单数据
                print(os.popen(self.py_path+" "+self.script_path+"NotionDBDA_BookList.py").read())

                return res

            except Exception as e:
                print(e)
                db.rollback()  #回滚事务

            finally:
                cursor.close() 
                db.close()  #关闭数据库连接
        
        return ""
    

    def NotionGetBookListByName(self,book_name):

        filter={
            "property":"书名",
            "title":{
                "equals":book_name
            }
        }

        results=NotionGetAllRecord(self.book_list_dbid,self.NotionAPI,self.token,self.Notion_Version,filter=filter)

        return results

    def NotionUpdateBookListByID(self,id,properties):

        url = self.NotionAPI+"pages/"+id

        payload = json.dumps({
        "parent": {
            "database_id": self.book_list_dbid
        },
        "properties": properties
        })
        headers = {
            "Authorization":"Bearer "+self.token,
            "Notion-Version":self.Notion_Version,
            "Content-Type":"application/json"
        }

        response = requests.request("PATCH", url, headers=headers, data=payload)

        print(response.text)
        