import requests
import json
import time
import operator
from datetime import datetime,timedelta
import pandas as pd 

# 解析标题字段
def NotionPropertyParse_Title(json_data):
    res=None
    if len(json_data["title"])!=0:
        res=json_data["title"][0]["plain_text"]
    return res

# 解析文本字段
def NotionPropertyParse_Text(json_data):
    res=None
    if len(json_data["rich_text"])!=0:
        res=""
        for s in json_data["rich_text"]:
            res+=s["plain_text"]
    return res

# 解析数字字段
def NotionPropertyParse_Number(json_data):
    res=json_data["number"]
    return res

# 解析日期字段，获取开始与结束日期
def NotionPropertyParse_Date(json_data):
    res=None
    if json_data["date"]!=None:
        res=[json_data["date"]["start"],json_data["date"]["end"]]
    return res

# 解析日期字段，获取开始日期
def NotionPropertyParse_Date_Start(json_data):
    res=None
    if json_data["date"]!=None:
        res=json_data["date"]["start"]
    return res

# 解析日期字段，获取结束日期
def NotionPropertyParse_Date_End(json_data):
    res=None
    if json_data["date"]!=None:
        res=json_data["date"]["end"]
    return res

# 解析单选字段
def NotionPropertyParse_Select(json_data):
    res=None
    if json_data["select"]!=None:
        res=json_data["select"]["name"]
    return res

# 解析多选字段
def NotionPropertyParse_Multi_Select(json_data):
    res=[]
    if len(json_data["multi_select"])!=0:
        for i  in json_data["multi_select"]:
            res.append(i["name"])
    return res

# 解析获取全表数据
def NotionGetAllRecord(dbid,NotionAPI,token,Notion_Version,filter=None,sorts=None):

    results=[]
    
    body={}
    if filter != None:
        body['filter']=filter
    if sorts != None:
        body['sorts']=sorts

    r=requests.request(
        "POST",
        NotionAPI+"databases/"+dbid+"/query",
        headers={
            "Authorization":"Bearer "+token,
            "Notion-Version":Notion_Version
        },
        json=body
        )
    
    json_data=json.loads(r.text)

    results+=json_data["results"]

    while json_data["has_more"]:
        start_cursor=json_data["next_cursor"]
        body={}
        if filter != None:
            body['filter']=filter
        if sorts != None:
            body['sorts']=sorts
        body['start_cursor']=start_cursor

        r=requests.request(
        "POST",
        NotionAPI+"databases/"+dbid+"/query",
        headers={
            "Authorization":"Bearer "+token,
            "Notion-Version":Notion_Version
        },
        json=body
        )
    
        json_data=json.loads(r.text)

        results+=json_data["results"]
    
    return results