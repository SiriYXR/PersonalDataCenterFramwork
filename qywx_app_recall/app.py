from flask import Flask,request,abort
from datetime import datetime
import threading
import time
import sys
import xml.etree.cElementTree as ET
from WXBizMsgCrypt3 import WXBizMsgCrypt
from MsgProcessor2 import MsgProcessor2
from QiYeWeiXinAPI import QYWXAPPSendText

app = Flask(__name__)
msgProcessor = MsgProcessor2()

sToken = "your_sToken" # 你的应用消息收发tocken
sEncodingAESKey = "your_sEncodingAESKey" #你的应用消息收发密钥
sCorpID = "your_sCorpID" #你的企业id

MAX_LENGTH=600

QYWXAPPSendText("服务启动成功！")

# 多线程并发处理消息
class MsgProcessThread (threading.Thread):
    def __init__(self,threadID,name,content):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.content=content
        
    def run(self):
        print ("开始线程：" + self.name)
        replyMsg=msgProcessor.processMsg(self.content)

        print(replyMsg)

        print("replyMsg length:",len(replyMsg))

        if(len(replyMsg)>0):
            msg=""
            for m in replyMsg.split("\n"):
                if len(msg)+len(m)>MAX_LENGTH:
                    QYWXAPPSendText(msg)
                    msg=m+"\n"
                else:
                    msg+=m+"\n"
            if(msg!=""):
                QYWXAPPSendText(msg)

        print ("退出线程：" + self.name)


# @app.route("/test/", methods=['GET'])
# def test():

#     content="日记"
#     threadID=datetime.now().strftime("%Y%m%d%H%M%S")
#     threadName="Thread-"+threadID
#     thread=MsgProcessThread(threadID,threadName,content)
#     thread.start()

#     return threadName

@app.route("/qywx_recall/siri_bird/", methods=['GET'])
def VerifyURL():

    msg_signature = request.args.get("msg_signature")
    timestamp = request.args.get("timestamp")
    nonce = request.args.get("nonce")
    echostr = request.args.get("echostr")

    print("msg_signature",msg_signature)
    print("timestamp",timestamp)
    print("nonce",nonce)
    print("echostr",echostr)
    print()

    wxcpt=WXBizMsgCrypt(sToken,sEncodingAESKey,sCorpID)
    
    ret,sEchoStr=wxcpt.VerifyURL(msg_signature, timestamp,nonce,echostr)
    if(ret!=0):
        print("ERR: VerifyURL ret: " + str(ret))
        abort(503)
    else:
        print("done VerifyURL")
        #验证URL成功，将sEchoStr返回给企业号
    
    print(sEchoStr)

    print("==============================")
    
    return sEchoStr

@app.route("/qywx_recall/siri_bird/", methods=['POST'])
def GetMsg():

    msg_signature = request.args.get("msg_signature")
    timestamp = request.args.get("timestamp")
    nonce = request.args.get("nonce")
    req_data= request.get_data()

    print("msg_signature",msg_signature)
    print("timestamp",timestamp)
    print("nonce",nonce)
    print("req_data",req_data)
    print()
    
    wxcpt=WXBizMsgCrypt(sToken,sEncodingAESKey,sCorpID)

    ret,sMsg=wxcpt.DecryptMsg( req_data, msg_signature, timestamp, nonce)
    print(ret,sMsg)
    if( ret!=0 ):
        print("ERR: DecryptMsg ret: " + str(ret))
        abort(503)
    # 解密成功，sMsg即为xml格式的明文
    xml_tree = ET.fromstring(sMsg)
    content = xml_tree.find("Content").text

    print("===========解密成功===================")

    print(content)

    print("===========处理===================")
    
    # 异步处理消息
    threadID=datetime.now().strftime("%Y%m%d%H%M%S")
    threadName="Thread-"+threadID
    thread=MsgProcessThread(threadID,threadName,content)
    thread.start()

    replyMsg=""

    print("===========响应===================")
    sRespData = "<xml><MsgType>text</MsgType><Content>"+replyMsg+"</Content><AgentID>1000002</AgentID></xml>"
    ret,sEncryptMsg=wxcpt.EncryptMsg(sRespData, nonce, timestamp)
    if( ret!=0 ):
        print("ERR: EncryptMsg ret: " + str(ret))
        abort(503)

    return sEncryptMsg