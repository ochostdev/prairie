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

client = MongoClient('mongodb://127.0.0.1:27017')
db = client.ocloud
#host="116.62.129.207"
host="18.18.110.220"
port=12354
addr=("0.0.0.0",port)
oldstate={}
#DPOSip="172.16.27.82"
DPOSip="18.18.251.7"
DPOSport=18889
filepath="/home/dockerfile/"
filestate={}

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
        while True:
            rec="norec"
            rec=client.recv(1024) 
            rec=rec.decode() 
            #print(rec)
            if rec.startswith("launchtask"):
                ret=socketsend(rec,host,port)
                #print(ret)
                if ret.startswith("launch111"):
                    client.send("launchsuccess".encode())
                else:
                    client.send("launchfailed".encode())
            else:
                #print("break")
                break
        client.close()
        return rec 
    except Exception as e:
        print(changered(hostip+":"+e))


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
                re["task"]={}
                ret2=db.tasklist.find({"slaveid":re["_id"],"status":1},{"_id":1,"taskid":1,"spendtime":1})
                for re2 in ret2:
                    db.tasklist.update_one({"_id":re2["_id"]},{"$set":{"spendtime":0}})
                    re["task"][re2["_id"]]=re2
                #local_log("re[state]:"+str(re["state"]))
                #local_log("ret2.count():"+str(ret2.count()))
                if re["state"]==1 and ret2.count()==0:
                    re["status"]=1
                else:
                    re["status"]=ret2.count()
                    db.Slave.update_one({"_id":re["_id"]},{"$set":{"state":0}})
                allslave.append(re)
            masnode["name"]="andy-master"
            masnode["ip"]=host
            allinfo["event"]="mastersync"
            allinfo["slave"]=allslave
            allinfo["masternode"]=masnode
            print(changegreen("post the salveinfo"+str(allinfo)))
            socketsend(str(allinfo),DPOSip,DPOSport)
            time.sleep(10)

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
                print(changeyellow("slave timeout deleted"+str(re)))
                local_log(changeyellow("slave timeout deleted"+str(re)))
            db.Slave.remove({"time":{"$lte":time.time()-15}})
            time.sleep(10)

class timekeeping(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            ret=db.tasklist.find({"$where":"this.renttime<=this.allspendtime"},{"_id":0,"taskid":1,"cmd":1,"dockername":1,"renttime":1,"allspendtime":1})
            for re in ret:
                print(changeyellow("task time finished"+str(re)))
                local_log(changeyellow("task time finished"+str(re)))
                #socketsend(str(re),DPOSip,DPOSport)
            if ret.count()!=0:
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
                print(changegreen("post the salveinfo"+str(allinfo)))
                socketsend(str(allinfo),DPOSip,DPOSport)
                ret=db.tasklist.remove({"$where":"this.renttime<=this.allspendtime"})
            time.sleep(30)

class Servers(SRH):
    def handle(self):
#        try:
            data = self.request.recv(4096)
            data=data.decode()
            #print(data)
            if data.startswith("resource"):
                print(changegreen("get slaveinfo"+data))
                dict = eval(data.strip("resource"))
                #print(dict)
                ret=db.Slave.find({"_id":dict['uuid']})
                if ret.count()==0:
                    db.Slave.save({"_id":dict['uuid'],"cpu":int(dict['cpu']),"gpu":int(dict['gpu']),"mem":int(dict['mem']),"disk":int(dict['rdisk']),"rcpu":int(dict['rcpu']),"rgpu":int(dict['rgpu']),"rmem":int(dict['rmem']),"rdisk":int(dict['rdisk']),"time":time.time(),"state":0})
                else:
                    db.Slave.update({"_id":dict['uuid']},{"$set":{"cpu":int(dict['cpu']),"gpu":int(dict['gpu']),"mem":int(dict['mem']),"disk":int(dict['rdisk']),"rcpu":int(dict['rcpu']),"rgpu":int(dict['rgpu']),"rmem":int(dict['rmem']),"rdisk":int(dict['rdisk']),"time":time.time()}})
                re=db.tasklist.find_one({"slaveid":dict["uuid"],"status":0},{"_id":1,"dockername1":1,"cmd1":1,"renttime":1,"taskid":1,"cpu":1,"mem":1,"gpu":1,"disk":1})
                if re:
                    print(changegreen("return "+dict['uuid']+"task"+str(re)))
                    local_log(changegreen("return "+dict['uuid']+"task"+str(re)))
                    self.request.send(str(re).encode())
                    db.tasklist.update({"_id":re["_id"]},{"$set":{"status":1}})
                    dictstate={}
                    dictstate["taskid"]=re["_id"]
                    dictstate["event"]="taskprocess"
                    dictstate["slaveid"]=dict["uuid"]
                    dictstate["process"]=8
                    socketsend(str(dictstate),DPOSip,DPOSport)
            elif data.startswith("launchtask"):
                launchtask=data
                dictstate={}
                print(changegreen("get a task from supernode"+data))
                local_log(changegreen("get a task from supernode"+data))
                self.request.send("launchsuccess".encode())
                taskinfo=eval(data.strip("launchtask"))
                result=""
                ret=db.Slave.find({"_id":taskinfo["slaveid"]})
                taskiduuid=taskinfo["subtaskid"]
                dictstate["taskid"]=taskiduuid
                dictstate["event"]="taskprocess"
                dictstate["slaveid"]=taskinfo["slaveid"]
                #===============insert into ================
                taskinfo["_id"]=taskiduuid
                taskinfo["status"]=1
                taskinfo["spendtime"]=0
                db.tasklist.save(taskinfo)
                self.request.send("launch111".encode())
                #===========================================
                if ret.count()==0:
                    result="slave error"
                else:
                    if not taskinfo["filemd5"] in filestate:
                        filestate[taskinfo["filemd5"]]=0
                        dictstate["process"]=0
                        socketsend(str(dictstate),DPOSip,DPOSport)
                    print("first:"+str(filestate))
                    if filestate[taskinfo["filemd5"]]!=0:
                        dictstate["process"]=filestate[taskinfo["filemd5"]]
                        socketsend(str(dictstate),DPOSip,DPOSport)
                        print(taskinfo["taskid"]+"env is standby")
                        local_log(taskinfo["taskid"]+"env is standby")
                        time.sleep(30)
                        socketsend(launchtask,host,port)
                    else:
                        db.Slave.update_one({"_id":taskinfo["slaveid"]},{"$set":{"state":1}})
                        ret=subprocess.getoutput("curl -u ocloud:ohcloud@123 http://127.0.0.1:5000/v2/_catalog")
                        if taskinfo["taskid"] in ret:
                            taskinfo["_id"]=taskiduuid
                            taskinfo["dockername1"]=host+":5000/"+taskinfo["taskid"]
                            if taskinfo["cmd"].find("[img]")==-1:
                                taskinfo["cmd1"]=taskinfo["cmd"]+" "+host+":5000/"+taskinfo["taskid"]
                            else:
                                taskinfo["cmd1"]=taskinfo["cmd"].replace("[img]",host+":5000/"+taskinfo["taskid"])
                            taskinfo["status"]=0
                            taskinfo["username"]="ocloud"
                            taskinfo["password"]="ohcloud@123"
                            taskinfo["remote"]=host+":5000"
                            taskinfo["spendtime"]=0
                            taskinfo["allspendtime"]=0
                            print("task:"+str(taskinfo))
                            db.tasklist.save(taskinfo)
                            result="gettag success"
                            filestate[taskinfo["filemd5"]]=0
                            dictstate["process"]=0
                            socketsend(str(dictstate),DPOSip,DPOSport)
                        else:
                            if filestate[taskinfo["filemd5"]]!=0:
                                dictstate["process"]=filestate[taskinfo["filemd5"]]
                                socketsend(str(dictstate),DPOSip,DPOSport)
                                print(taskinfo["taskid"]+"env is standby")
                                local_log(taskinfo["taskid"]+"env is standby")
                                time.sleep(30)
                                socketsend(launchtask,host,port)
                            else:
                                #=========================2 no such file==========================
                                filestate[taskinfo["filemd5"]]=2
                                dictstate["process"]=2
                                socketsend(str(dictstate),DPOSip,DPOSport)
                                if not os.path.exists(filepath+taskinfo["dockername"]):
                                    local_log(changered("dockerfile does not exist"))
                                    print("nofile")
                                    result="needfile"
                                else:
                                    #===================================3 md4sum error========================================
                                    filestate[taskinfo["filemd5"]]=3
                                    dictstate["process"]=3
                                    socketsend(str(dictstate),DPOSip,DPOSport)
                                    if subprocess.getstatusoutput("md5sum "+filepath+taskinfo["dockername"]+" | awk '{printf $1}'")[1]!=taskinfo["filemd5"]:
                                        local_log(changered("docker md5 verify failed"))
                                        print("md5error")
                                        result="needfile"
                                    else:
                                        #===================================4 docker load========================================
                                        filestate[taskinfo["filemd5"]]=4
                                        dictstate["process"]=4
                                        socketsend(str(dictstate),DPOSip,DPOSport)
                                        ret=subprocess.getoutput("docker load < "+filepath+taskinfo["dockername"])
                                        time.sleep(20)
                                        print("1:"+ret)
                                        local_log("1:"+ret)
                                        if "Loaded image" not in ret:
                                            result="docker load error"
                                            filestate[taskinfo["filemd5"]]=0
                                        else:
                                            #===============================5 docker tag=========================================
                                            filestate[taskinfo["filemd5"]]=5
                                            dictstate["process"]=5
                                            socketsend(str(dictstate),DPOSip,DPOSport)
                                            if "image:" in ret:
                                                dockername=ret.split("image: ")[1]
                                            elif "ID:" in ret:
                                                dockername=ret.split("ID: ")[1]
                                            tag1=taskinfo["taskid"]
                                            tag2=host+":5000/"+tag1
                                            ret=subprocess.getoutput("docker tag "+dockername+" "+tag1)
                                            ret=subprocess.getoutput("docker tag "+tag1+" "+tag2)
                                            print("2:"+ret)
                                            local_log("2:"+ret)
                                            if ret!="":
                                                result="docker tag error"
                                                filestate[taskinfo["filemd5"]]=0
                                            else:
                                                #================================6 docker login=====================================
                                                filestate[taskinfo["filemd5"]]=6
                                                dictstate["process"]=6
                                                socketsend(str(dictstate),DPOSip,DPOSport)
                                                (status,ret)=subprocess.getstatusoutput("docker login -u ocloud -p ohcloud@123 127.0.0.1:5000")
                                                print("3:"+ret)
                                                local_log("3:"+ret)
                                                if status !=0:
                                                    result="docker login error"
                                                    filestate[taskinfo["filemd5"]]=0
                                                else:
                                                    #===============================7 docker push====================================
                                                    filestate[taskinfo["filemd5"]]=7
                                                    dictstate["process"]=7
                                                    socketsend(str(dictstate),DPOSip,DPOSport)
                                                    print("push tag:"+tag2)
                                                    local_log("push tag:"+tag2)
                                                    ret=subprocess.getoutput("docker push "+tag2)
                                                    print("4:"+ret)
                                                    local_log("4:"+ret)
                                                    if "latest: digest:" not in ret:
                                                        result="docker push error"
                                                        filestate[taskinfo["filemd5"]]=0
                                                    else:
                                                        os.system("docker logout 127.0.0.1:5000")
                                                        print("5:everything is ok")
                                                        taskinfo["_id"]=taskiduuid
                                                        taskinfo["dockername1"]=tag2
                                                        if taskinfo["cmd"].find("[img]")==-1:
                                                            taskinfo["cmd1"]=taskinfo["cmd"]+" "+tag2
                                                        else:
                                                            taskinfo["cmd1"]=taskinfo["cmd"].replace("[img]",tag2)
                                                        taskinfo["status"]=0
                                                        taskinfo["username"]="ocloud"
                                                        taskinfo["password"]="ohcloud@123"
                                                        taskinfo["remote"]=host+":5000"
                                                        taskinfo["spendtime"]=0
                                                        taskinfo["allspendtime"]=0
                                                        print("task is"+str(taskinfo))
                                                        db.tasklist.save(taskinfo)
                                                        result="gettag success"
                                                        filestate[taskinfo["filemd5"]]=0
                                                        dictstate["process"]=0
                                                        socketsend(str(dictstate),DPOSip,DPOSport)
                if result.startswith("needfile"):
                    print(changered(result))
                    local_log(changered(result))
                    client=socket(AF_INET,SOCK_STREAM)
                    client.connect((DPOSip,DPOSport))
                    client.settimeout(10)
                    data1={}
                    data1["event"]="needfile"
                    data1["taskid"]=taskinfo["taskid"]
                    data1["dockername"]=taskinfo["dockername"]
                    client.send(str(data1).encode())
                    filestate[taskinfo["filemd5"]]=1
                    dictstate["process"]=1
                    socketsend(str(dictstate),DPOSip,DPOSport)
                    socketsend(str(taskinfo["taskid"])+"#1#sending file",DPOSip,DPOSport)
                    with open(filepath+taskinfo["dockername"],'wb') as f:
                        print('file opened')
                        while True:
                            data = client.recv(1024)
                            if not data:
                                break
                            f.write(data)
                    print('filerecvclose')
                    f.close()
                    filestate[taskinfo["filemd5"]]=0
                    dictstate["process"]=0
                    db.tasklist.remove({"_id":taskiduuid})
                    socketsend(str(dictstate),DPOSip,DPOSport)
                    socketsend(launchtask,host,port)
                elif result.startswith("gettag success"):
                    pass
                else:
                    print(changered(result))
                    local_log(changered(result))
                    db.tasklist.remove({"_id":taskiduuid})
            elif data.startswith("taskrunning"):
                print(changegreen("task is timing"+data))
                data=data.strip("taskrunning")
                dict=eval(data)
                db.tasklist.update_one({"_id":dict["taskid"]},{"$set":{"spendtime":dict["spendtime"]},"$inc":{"allspendtime":dict["spendtime"]}})
                re=db.tasklist.find_one({"_id":dict["taskid"]},{"taskid":1})
                if re:
                    dictstate={}
                    dictstate["taskid"]=re["taskid"]
                    dictstate["event"]="taskprocess"
                    dictstate["slaveid"]=dict["uuid"]
                    dictstate["process"]=0
                    socketsend(str(dictstate),DPOSip,DPOSport)
            elif data.startswith("taskfailed"):
                print(changered("task is failed"+data))
                local_log(changered(data))
                data=data.strip("taskfailed")
                dict=eval(data)
                try:
                    db.Slave.update_one({"_id":dict["uuid"]},{"$set":{"state":0}})
                    #db.tasklist.update_one({"_id":dict["taskid"]},{"$set":{"spendtime":dict["spendtime"]},"$inc":{"allspendtime":dict["spendtime"]}})
                    #ret=db.tasklist.find({"_id":dict["taskid"]},{"_id":1,"cmd":1,"dockername":1,"renttime":1,"allspendtime":1})
                    #print(ret)
                    #for re in ret:
                    #socketsend(str(re),DPOSip,DPOSport)
                    #print(re["_id"])
                    db.tasklist.remove({"_id":dict["taskid"]})
                except Exception as e:
                    print(str(e))
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

