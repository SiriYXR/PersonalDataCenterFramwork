from urllib.parse import unquote

from MsgAct_Help import *
from MsgAct_ReadRecord import *

class MsgProcessor2():
    def __init__(self):

        # 交互动作注册列表
        self.actionList=[
                        {"name":"帮助","action":HelpAction(self)},
                        {"name":"阅读记录","action":ReadRecordAction()}
                        ]

    def processMsg(self,content):
        reMsg=[]

        cmdList=content.split("\n") # 按行切割消息
        for i in cmdList:
            i=i.strip() # 去除收尾空格

            # 跳过空行
            if(len(i)==0):
                continue
            
            res=self.doAction(i) # 调用消息处理函数
            
            if(res!=""):
                reMsg.append(res) # 收集消息处理结果
        
        if(len(reMsg)==0):
            return ""
        return "\n\n".join(reMsg).strip() # 将结果拼接并返回


    def doAction(self,content):
        contents=[ unquote(i.strip()) for i in content.split(" ") if len(i.strip())>0] # 将命令转化为参数列表格式

        for act in self.actionList:
            res=act["action"].doAction(contents) # 调用各模块的消息处理函数
            if(res!=""):
                return res

        return ""

# 用于测试
if __name__ == "__main__":
    msgProcessor=MsgProcessor2()

    msg="""
    帮助
    """

    print(msgProcessor.processMsg(msg))