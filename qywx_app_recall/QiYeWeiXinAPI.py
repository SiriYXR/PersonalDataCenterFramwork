import requests
import json

"""
发送Text消息到企业微信应用
"""
def QYWXAPPSendText(content):
    
    # 获取tocken
    tocken_url="https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid"+"你的企业id"+"&corpsecret="+"你的应用密钥"

    response = requests.request("GET", tocken_url)

    js=json.loads(response.text)

    tocken=""
    if(js["errmsg"]=="ok"):
        tocken=js["access_token"]
    else:
        print("企业微信应用tocken获取失败！")
        return

    # 发送消息

    url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token="+tocken

    payload = json.dumps({
    "touser" : "@all",
    "msgtype" : "text",
    "agentid" : 1000002, # 你的应用id
    "text":{
        "content":content
    }
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)