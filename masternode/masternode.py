import socket
import socketserver
from socket import *
from socketserver import StreamRequestHandler as SRH
import time
import subprocess
from time import clock
import paho.mqtt.client as mqtt
import json
import os
import threading
from threading import Timer
from pymongo import MongoClient
from array import array
import struct
import subprocess
import logging
import uuid
from logging.handlers import TimedRotatingFileHandler

client = MongoClient('mongodb://localhost:27017')
db = client.ocloud
host="18.18.110.220"
port=12354
addr=(host,port)
oldstate={}
DPOSip="18.18.25.29"
DPOSport=18889
filepath="/home/dockerfile/"
filestate={}
dockerstate={}

def init_logger():
    logger = logging.getLogger("ocbclog")
    logger.setLevel(logging.DEBUG)
    if not os.path.isdir("/var/log/"):
        os.system("mkdir /var/log/")
    handler = TimedRotatingFileHandler("/var/log/ocbclog.log",when='midnight',backupCount=31)
    datefmt = '%Y-%m-%d %H:%M:%S'
    format_str = '[%(asctime)s] %(message)s'
    formatter = logging.Formatter(format_str, datefmt)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger
logger = init_logger()

def local_log(message):
    logger.info(message)

def changered(str):
    return "\033[1;31;40m"+str+"\033[0m"

def changegreen(str):
    return "\033[1;32;40m"+str+"\033[0m"

def changeyellow(str):
    return "\033[1;33;40m"+str+"\033[0m"

def socketsend(data,hostip,hostport):
    try:
        client=socket(AF_INET,SOCK_STREAM)
        client.connect((hostip,hostport))
        client.settimeout(10)
        data=data.encode()
        client.send(data)
        client.close()
    except Exception as e:
        print(changered(hostip+"通讯异常"))


#-----定时上传节点信息-------
class sendslave(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            allslave=[]
            allinfo={}
            masnode={}
            ret=db.Slave.find()
            for re in ret:
                alltask=[]
                ret2=db.tasklist.find({"slaveid":re["_id"],"status":1},{"_id":1,"taskid":1,"spendtime":1})
                for re2 in ret2:
                    alltask.append(re2)
                    db.tasklist.update_one({"_id":re2["_id"]},{"$set":{"spendtime":0}})
                #local_log("re[state]:"+str(re["state"]))
                #local_log("ret2.count():"+str(ret2.count()))
                if re["state"]==1 and ret2.count()==0:
                    re["status"]=1
                else:
                    re["status"]=ret2.count()
                    db.Slave.update_one({"_id":re["_id"]},{"$set":{"state":0}})
                re["task"]=alltask
                allslave.append(re)
            masnode["name"]="andy-master"
            allinfo["event"]="mastersync"
            allinfo["slave"]=allslave
            allinfo["masternode"]=masnode
            print(changegreen("节点信息上传："+str(allinfo)))
            socketsend(str(allinfo),DPOSip,DPOSport)
            time.sleep(10)

#------定时删除超时节点-------
class deletedeadslave(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            ret=db.Slave.find({"time":{"$lte":time.time()-15}},{"_id":1})
            for re in ret:
                res=db.tasklist.find({"uuid":re["_id"]})
                for rs in res:
                    socketsend(str(rs),DPOSip,DPOSport) 
                db.tasklist.remove({"slaveid":re["_id"]})
                print(changeyellow("节点超时删除："+str(re)))
                local_log(changeyellow("节点超时删除："+str(re)))
            db.Slave.remove({"time":{"$lte":time.time()-15}})
            time.sleep(10)

#------定时对任务进行计时--------
class timekeeping(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            ret=db.tasklist.find({"$where":"this.renttime<=this.allspendtime"},{"_id":0,"taskid":1,"cmd":1,"dockername":1,"renttime":1,"allspendtime":1})
            for re in ret:
                print(changeyellow("任务时间大于预设时间:"+str(re)))
                local_log(changeyellow("任务时间大于预设时间:"+str(re)))
                socketsend(str(re),DPOSip,DPOSport)
            if ret.count()!=0:
                ret=db.tasklist.remove({"$where":"this.renttime<=this.allspendtime"})
            time.sleep(10)

class Servers(SRH):
    def handle(self):
#        try:
            data = self.request.recv(4096)
            data=data.decode()
            #print(data)
            if data.startswith("resource"):
                print(changegreen("收到节点资源信息："+data))
                dict = eval(data.strip("resource"))
                #print(dict)
                ret=db.Slave.find({"_id":dict['uuid']})
                if ret.count()==0:
                    db.Slave.save({"_id":dict['uuid'],"cpu":int(dict['cpu']),"gpu":int(dict['gpu']),"mem":int(dict['mem']),"time":time.time(),"state":0})
                else:
                    db.Slave.update({"_id":dict['uuid']},{"$set":{"cpu":int(dict['cpu']),"gpu":int(dict['gpu']),"mem":int(dict['mem']),"time":time.time()}})
                re=db.tasklist.find_one({"slaveid":dict["uuid"],"status":0},{"_id":1,"dockername1":1,"cmd1":1,"renttime":1})
                if re:
                    print(changegreen("返回节点"+dict['uuid']+"的任务："+str(re)))
                    local_log(changegreen("返回节点"+dict['uuid']+"的任务："+str(re)))
                    self.request.send(str(re).encode())
                    db.tasklist.update({"_id":re["_id"]},{"$set":{"status":1}})
            elif data.startswith("launchtask"):
                launchtask=data
                print(changegreen("收到云端下发任务："+data))
                local_log(changegreen("收到云端下发任务："+data))
                self.request.send("launchsuccess".encode())
                taskinfo=eval(data.strip("launchtask"))
                result=""
                ret=db.Slave.find({"_id":taskinfo["slaveid"]})
                if ret.count()==0:
                    result="slave节点异常"
                else:
                    db.Slave.update_one({"_id":taskinfo["slaveid"]},{"$set":{"state":1}})
                    ret=subprocess.getoutput("docker images | grep -v CREATED | grep "+taskinfo["taskid"])
                    if ret:
                        taskinfo["_id"]=taskinfo["taskid"]+str(uuid.uuid1())
                        taskinfo["dockername1"]=host+":5000/"+taskinfo["taskid"]
                        if taskinfo["cmd"].find("yunyo")==-1:
                            taskinfo["cmd1"]=taskinfo["cmd"]+" "+host+":5000/"+taskinfo["taskid"]
                        else:
                            taskinfo["cmd1"]=taskinfo["cmd"].replace("yunyo",host+":5000/"+taskinfo["taskid"])
                        taskinfo["status"]=0
                        taskinfo["username"]="ocloud"
                        taskinfo["password"]="ohcloud@123"
                        taskinfo["remote"]=host+":5000"
                        taskinfo["spendtime"]=0
                        taskinfo["allspendtime"]=0
                        print("任务为："+str(taskinfo))
                        db.tasklist.insert(taskinfo)
                        result="gettag success"
                    else:
                        if not os.path.exists(filepath+taskinfo["dockername"]):
                            local_log(changered("docker文件不存在"))
                            result="needfile"
                        else:
                            if subprocess.getstatusoutput("md5sum "+filepath+taskinfo["dockername"]+" | awk '{printf $1}'")[1]!=taskinfo["filemd5"]:
                                local_log(changered("docker文件md5校验失败"))
                                if filestate[taskinfo["taskid"]]==1:
                                    print(taskinfo["taskid"]+"docker文件正在传输等待中")
                                    local_log(taskinfo["taskid"]+"docker文件正在传输等待中")
                                    time.sleep(1)
                                    socketsend(launchtask,host,port)
                                else:
                                    result="needfile"
                            else:
                                if dockerstate[taskinfo["taskid"]]==1:
                                    print(taskinfo["taskid"]+"docker正在push等待中")
                                    local_log(taskinfo["taskid"]+"docker正在push等待中")
                                    time.sleep(1)
                                    socketsend(launchtask,host,port)
                                else:
                                    dockerstate[taskinfo["taskid"]]=1
                                    ret=subprocess.getoutput("docker load < "+filepath+taskinfo["dockername"])
                                    print("1:"+ret)
                                    if "Loaded image:" not in ret:
                                        result="docker load异常"
                                        dockerstate[taskinfo["taskid"]]=0
                                    else:
                                        dockername=ret.split("image: ")[1]
                                        tag1=taskinfo["taskid"]
                                        tag2=host+":5000/"+tag1
                                        ret=subprocess.getoutput("docker tag "+dockername+" "+tag1)
                                        ret=subprocess.getoutput("docker tag "+tag1+" "+tag2)
                                        print("2:"+ret)
                                        if ret!="":
                                            result="docker tag异常"
                                            dockerstate[taskinfo["taskid"]]=0
                                        else:
                                            ret=subprocess.getoutput("docker login -u ocloud -p ohcloud@123 "+host+":5000")
                                            print("3:"+ret)
                                            if ret !="Login Succeeded":
                                                result="docker login异常"
                                                dockerstate[taskinfo["taskid"]]=0
                                            else:
                                                ret=subprocess.getoutput("docker push "+tag2)
                                                print("4:"+ret)
                                                if "latest: digest:" not in ret:
                                                    result="docker push异常"
                                                    dockerstate[taskinfo["taskid"]]=0
                                                else:
                                                    os.system("docker logout "+host+":5000")
                                                    print("5:everything is ok")
                                                    dockerstate[taskinfo["taskid"]]=0
                                                    taskinfo["_id"]=tag1+str(uuid.uuid1())
                                                    taskinfo["dockername1"]=tag2
                                                    if taskinfo["cmd"].find("yunyo")==-1:
                                                        taskinfo["cmd1"]=taskinfo["cmd"]+" "+tag2
                                                    else:
                                                        taskinfo["cmd1"]=taskinfo["cmd"].replace("yunyo",tag2)
                                                    taskinfo["status"]=0
                                                    taskinfo["username"]="ocloud"
                                                    taskinfo["password"]="ohcloud@123"
                                                    taskinfo["remote"]=host+":5000"
                                                    taskinfo["spendtime"]=0
                                                    taskinfo["allspendtime"]=0
                                                    print("任务为："+str(taskinfo))
                                                    db.tasklist.insert(taskinfo)
                                                    result="gettag success"
                if result.startswith("needfile"):
                    print(changered(result))
                    local_log(changered(result))
                    client=socket(AF_INET,SOCK_STREAM)
                    client.connect((DPOSip,DPOSport))
                    client.settimeout(10)
                    data1={}
                    data1["event"]="needfile"
                    data1["taskid"]=taskinfo["taskid"]
                    client.send(str(data1).encode())
                    with open(filepath+taskinfo["dockername"],'wb') as f:
                        print('file opened')
                        filestate[taskinfo["taskid"]]=1
                        dockerstate[taskinfo["taskid"]]=0
                        while True:
                            data = client.recv(1024)
                            if not data:
                                break
                            f.write(data)
                    print('filerecvclose')
                    filestate[taskinfo["taskid"]]=0
                    f.close()
                    socketsend(launchtask,host,port)
                else:
                    print(changered(result))
                    local_log(changered(result))
            elif data.startswith("taskrunning"):
                print(changegreen("收到任务正在运行的计时："+data))
                data=data.strip("taskrunning")
                dict=eval(data)
                db.tasklist.update_one({"_id":dict["taskid"]},{"$set":{"spendtime":dict["spendtime"]},"$inc":{"allspendtime":dict["spendtime"]}})
            elif data.startswith("taskfailed"):
                print(changered("收到任务异常的时间："+data))
                local_log(changered(data))
                data=data.strip("taskfailed")
                dict=eval(data)
                db.Slave.update_one({"_id":taskinfo["slaveid"]},{"$set":{"state":0}})
                db.tasklist.update_one({"_id":dict["taskid"]},{"$set":{"spendtime":dict["spendtime"]},"$inc":{"allspendtime":dict["spendtime"]}})
                ret=db.tasklist.find({"_id":dict["taskid"]},{"_id":1,"cmd":1,"dockername":1,"renttime":1,"allspendtime":1})
                for re in ret:
                    socketsend(str(re),DPOSip,DPOSport)
                    db.tasklist.remove({"_id":re["_id"]})
#        except Exception as e:
#            local_log(changered("error:"+str(e)))


if __name__== "__main__":
    db.tasklist.drop()
    db.Slave.drop()
    delete=deletedeadslave()
    delete.start()
    send=sendslave()
    send.start()
    tking=timekeeping()
    tking.start()
    server=socketserver.ThreadingTCPServer(addr,Servers)
    server.serve_forever()
