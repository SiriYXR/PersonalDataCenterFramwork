
"""
s: 原始字符串
l: 填充后的长度
n: 单位长度换算的空格数
b: 填充的方向
"""
def fillSpace(s,l,n=1,b=True):
    if(len(s)<l):
        if b:
            s=s+" "*(l-len(s))*n
        else:
            s=" "*(l-len(s))*n+s
    return s

def time_format(minute):
    minute=int(minute)
    h=int(minute/60)
    m=minute%60
    res=""
    if(h>0):
        res+=str(h)+"小时"
    res+=str(m)+"分钟"

    return res