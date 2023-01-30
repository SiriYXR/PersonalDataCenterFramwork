class HelpAction():

    def __init__(self,father):
        self.father=father
        pass 
        
    def doAction(self,args):
        res_info=""
        if len(args)==1 and (args[0]=="帮助" or args[0]=="help"):
            res_info="帮助文档\n\n目前已集成的模块:\n"
            for act in self.father.actionList:
                res_info+="* "+act["name"]+"\n"
            res_info+="\n输入“帮助 模块名”查看详细信息"
        return res_info

        return ""