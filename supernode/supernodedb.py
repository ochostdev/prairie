#from pymongo import MongoClient
from socketserver import StreamRequestHandler as SRH
import threading
import socketserver
import socket
import time
import subprocess
import json
import os


mongouri='mongodb://127.0.0.1:27017/ocbc'
a=MongoClient(mongouri)
b=a.ocbc
c=b.ocbc
ret=c.find()
for i in ret:
    print(i)

addr=("0.0.0.0",18889)

masters={}#
slaves={}#
slavesdoing={}
taskrescanlist={}
tasklist={}#
synclist={}
synclist_m={}
locklist={}
locklist["sync"]=False
locklist["syncm"]=False
pendinglist=[]#

slavenodeformat={"_id":0,"cpu":0,"rcpu":0,"gpu":0,"rgpu":0,"mem":0"rmem":0"disk":0,"rdisk":0,"task":0,"master":0}

def slavenodeformatcheck(slaves):
    for i in slavenodeformat:
        if i not in slaves:
            return False
    return True



class checkstate(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            try:
                while True:
                    if locklist["sync"]:
                        time.sleep(1)
                    else:
                        locklist["sync"]=True
                        break
                deletelist=[]
                deletelist_m=[]
                taskrelist=[]
                taskrelist_m=[]
                for ii in synclist:
                    if synclist[ii]==0:
                        synclist[ii]=-1
                    elif synclist[ii]==-1:
                        taskrelist.append(ii)
                        deletelist.append(ii)
                        synclist[ii]=-2
                for ii in synclist_m:
                    if synclist_m[ii]==0:
                        synclist_m[ii]=-1
                    elif synclist_m[ii]==-1:
                        taskrelist_m.append(ii)
                        deletelist_m.append(ii)
                        synclist_m[ii]=-2
                #print(taskrelist_m)
                for j in taskrelist_m:
                    for x in slaves:
                        if slaves[x]["master"]==j:
                            #print(slaves[x]["task"])
                            #for y in slaves[x]["task"]:
                            if slaves[x]["requesttask"]!="":
                                if slaves[x]["renttime"]>0:
                                    print("taskstate-m-pendinglist")
                                    pendinglist.append({"renttime":slaves[x]["renttime"],"taskid":slaves[x]["requesttask"]})
                                    taskrelist.remove(x)
                #print(taskrelist)
                for kk in taskrelist:
                    #for x in slaves[kk]["task"]:
                    if slaves[kk]["requesttask"]!="":
                        if slaves[kk]["renttime"]>0:
                            print("taskstate-pendinglist")
                            pendinglist.append({"renttime":slaves[kk]["renttime"],"taskid":slaves[kk]["requesttask"]})
                for j in deletelist:
                    del slaves[j]
                    print("slaves["+j+"]deleted")
                for j in deletelist_m:
                    del masters[j]
                    print("masters["+j+"]deleted")
            except Exception as e:
                print(e)
            finally:
                locklist["sync"]=False
                time.sleep(12)



class taskrescan(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        #pendinglist={}
        #leavelist=[]
        while True:
            leavelist=[]
            try:
                while True:
                    if locklist["sync"]:
                        time.sleep(1)
                    else:
                        locklist["sync"]=True
                        break
        ret=slavelist.find({"rcpu":{"$gte":retjson["cpu"]},"rgpu":{"$gte":retjson["gpu"]},"rmem":{"$gte":retjson["mem"]},"rdisk":{"$gte":retjson["disk"]}})
                                if ret==None:

                for j in pendinglist:
                    for ii in slaves:
                        if slaves[ii]["taskstate"]=="avaliable":
                            if ii not in slavesdoing:
                                slavesdoing[ii]=0
                            if slavesdoing[ii]==0:
                                if slaves[ii]["cpu"]>=tasklist[j["taskid"]]["cpu"]:
                                    if slaves[ii]["mem"]>=tasklist[j["taskid"]]["mem"]:
                                        if slaves[ii]["gpu"]>=tasklist[j["taskid"]]["gpu"]:
                                            if informmaster(masters[slaves[ii]["master"]],ii,j["renttime"],j["taskid"]):
                                                slavesdoing[ii]=1
                                                leavelist.append(j)
                                                slaves[ii]["taskstate"]="pending"
                                                slaves[ii]["renttime"]=j["renttime"]
                                                slaves[ii]["requesttask"]=j["taskid"]
                                                break
                for i in leavelist:
                    pendinglist.remove(i)
            except Exception as e:
                print(e)
            finally:
                locklist["sync"]=False
            time.sleep(10)


class refreshdoinglist(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        templist={}
        while True:
            for i in slavesdoing:
                if slavesdoing[i]>0:
                    slavesdoing[i]+=1
                if slavesdoing[i]>10:
                    slavesdoing[i]=0
            time.sleep(3)


def flushtofile():
    fd=open("supernode.save","wt")
    fd.write(str(masters))
    fd.write("\n")
    fd.write(str(slaves))
    fd.write("\n")
    fd.write(str(tasklist))
    fd.write("\n")
    fd.write(str(pendinglist))
    fd.write("\n")
    fd.close()


#def startupfromfile():



def informmaster(masterip,slaveid,renttime,taskid):
    client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        client.connect((masterip,12354))
        client.settimeout(60)
        temp={}
        temp["cmd"]=tasklist[taskid]["cmd"]
        temp["dockername"]=tasklist[taskid]["dockername"]
        temp["slaveid"]=slaveid
        temp["renttime"]=renttime
        temp["taskid"]=taskid
        temp["filemd5"]=tasklist[taskid]["filemd5"]
        client.send(("launchtask"+str(temp)).encode())
        b=client.recv(64)
        ret=b.decode()
        #if ret=="":
        client.close()
        if ret=="launchsuccess":
            return True
        else:
            print("what?ret="+ret)
            return False
    except:
        print("CONNECTION_FAILED")
        return False


def launchtask(retjson):
        try:
            while True:
                if locklist["sync"]:
                    time.sleep(1)
                else:
                    locklist["sync"]=True
                    break
            for i in range(0,retjson["num"]):
                print("taskrescan-l-pendinglist")
                pendinglist.append({"renttime":retjson["renttime"],"taskid":retjson["taskid"]})
            locklist["sync"]=False
            return 0
        except Exception as e:
            print(e)
            locklist["sync"]=False
            return 1
        finally:
            locklist["sync"]=False
        #time.sleep(12)

def launchtaskdb(retjson):
    try:
        a=MongoClient(mongouri)
        b=a.prairie
        pendinglist=b.pendinglist
        tasklist=b.tasklist
        tasklist.insert(retjson)
        for i in range(0,retjson["num"]):
            pendinglist.insert({"_id":retjson["_id"],"renttime":retjson["renttime"]})
        return 0
    except Exception as e:
        print(e)
        return 1



class Servers(SRH):
    def handle(self):
        data = self.request.recv(1024)
        data=data.decode()
        #print(data)
        retjson=""
        try:
            retjson=eval(data)
        except:
            print("json format error")
        if "event" in retjson:
            if retjson["event"]=="launchtask": #json:{"cmd":"","dockername":"filename","event":"launchtask","cpu":1,"mem":1.0,"gpu":1,num:"3","renttime":10000,"_id":"18xxxx"}
            #basic check
                if "cpu" in retjson and "mem" in retjson and "gpu" in retjson:
                    #update mongodb
                    retjson["dep"]="no"
                    if retjson["taskid"] in tasklist:
                        print("dump taskid")
                    else:
                        output=subprocess.getoutput("md5sum /var/www/html/dist/"+retjson["dockername"]+" |awk '{printf $1}'")
                        retjson["filemd5"]=output
                        #tasklist[retjson["taskid"]]=retjson
                        #tasklist[retjson["taskid"]]["alltime"]=retjson["renttime"]*retjson["num"]
                        ret=str(launchtask(retjson))
                        self.request.send(ret.encode())
            elif retjson["event"]=="taskprocess":
                if retjson["slaveid"] in slaves:
                    flag=0
                    for i in slaves[retjson["slaveid"]]["taskprocess"]:
                        if i.startswith(retjson["taskid"]):
                            if retjson["process"]==0:
                                slaves[retjson["slaveid"]]["taskprocess"].remove(i)
                            else:
                                slaves[retjson["slaveid"]]["taskprocess"].remove(i)
                                slaves[retjson["slaveid"]]["taskprocess"].append(retjson["taskid"]+"@"+str(retjson["process"]))
                            flag=1
                    if flag==0:
                        if retjson["process"]!=0:
                            slaves[retjson["slaveid"]]["taskprocess"].append(retjson["taskid"]+"@"+str(retjson["process"]))
            elif retjson["event"]=="mastersync":#mastersync:_id rdisk slaves
                try:
                    #lock start
                    while True:
                        if locklist["sync"]:
                            time.sleep(1)
                        else:
                            locklist["sync"]=True
                            break
                    #check _id rdisk slaves   0:pass 1:pass-dupid 2:format error
                    checkstate=0
                    a=MongoClient(mongouri)
                    b=a.prairie
                    masternodelist=b.masternode
                    if "_id" in retjson and "rdisk" in retjson and "slaves" in retjson:
                        ret=masternodelist.find({"_id":retjson["_id"]})
                        if ret!=None:
                            checkstate=1
                    else:
                        checkstate=2
                        print("formaterror:"+str(retjson))



                    #check complete,start write to db  masternodeindb:_id rdisk ip
                    if checkstate==0:
                        #for i in retjson["slaves"]:
                        masternodelist.insert({"_id":retjson["_id"],"rdisk":retjson["rdisk"],"ip":self.client_address[0]})
                    elif checkstate==1:
                        masternodelist.update({"_id":retjson["_id"]},{"$set",{"rdisk":retjson["rdisk"],"ip":self.client_address[0]}},False,True)

                    #writed to db start check slaves
                    synclist_m[retjson["masternode"]["name"]]=0
                    slavenodelist=b.slavenode
                    taskindoing=b.taskindoing

                    slaves=slavenodelist.find({"master":retjson["_id"]})
                    tasks=
                    for i in retjson["slave"]:

                        #format checkstate
                        if not slavenodeformatcheck(i):
                            print("slavenodeformaterror:"+str(i))
                            continue

                        synclist[i["_id"]]=0

                        #find the slavenode
                        slaveindb=None
                        for j in slaves:
                            if j["_id"]==i["id"]:
                                slaveindb=j
                                break
                        if slaveindb==None:

                            continue
                        else:
                            taskdet=[] #temp slavenode's tasklist
                            taskdetindb=slaveindb["task"]#task in db
                            #check task spendtime
                            if i["task"]==[]:

                            else:
                                for x in i["task"]:
                                    temptask=i["task"][x]
                                    if "_id" in temptask and "status" in temptask and "spendtime" in temptask and "taskid" in temptask:
                                        if temptask["_id"] in taskdetindb:       #this task is in db
                                            remaintime=taskdetindb[temptask["_id"]]["remaintime"]-temptask["spendtime"]
                                            if remaintime<=0:
                                                print("slavenode-"+i["_id"]+"-taskid-"+temptask["_id"]+"-finishtime")
                                            taskdetindb[temptask["_id"]]["remaintime"]=remaintime
                                            taskdetindb[temptask["_id"]]["status"]=temptask["status"]
                                        else:     #this task is not in




                        if i["status"]>0:
                            i["taskstate"]="working"
                        else:
                            i["taskstate"]="avaliable"

                        if i["_id"] not in slaves:#slavenode not in db

                            slavenodelist.insert(i)
                        else: #slavenode in db
                            slavenodelist.update()


                        slavetask=[]
                        for x in taskdet:
                            if "_id" in x:
                                slavetask.append(x["taskid"])
                                slaves[i["_id"]]["renttime"]=slaves[i["_id"]]["renttime"]-x["spendtime"]
                                tasklist[x["taskid"]]["alltime"]=tasklist[x["taskid"]]["alltime"]-x["spendtime"]
                                if slaves[i["_id"]]["renttime"]<=0:
                                    print(i["_id"]+":finishtime")
                        if taskdet==[] and slaves[i["_id"]]["renttime"]>0 and slaves[i["_id"]]["task"]!=[] and slaves[i["_id"]]["requesttask"]!="":
                            print("taskrescan-s-pendinglist")
                            pendinglist.append({"taskid":slaves[i["_id"]]["requesttask"],"renttime":slaves[i["_id"]]["renttime"]})
                            slaves[i["_id"]]["spendtime"]=0
                        slaves[i["_id"]]["task"]=slavetask
                            #slaves[i["_id"]]["taskstate"]="avaliable"
                        #slavesdoing[i["_id"]]=0
                except Exception as e:
                        print("whaterror")
                finally:
                        locklist["sync"]=False
            elif retjson["event"]=="needfile":
                if "taskid" in retjson:
                    f = open("/var/www/html/dist/"+retjson["dockername"],'rb')
                    try:
                        l = f.read(1024)
                        while(l):
                            self.request.send(l)
                            #print("sent ",repr(l))
                            l=f.read(1024)
                    except:
                        print("send error")
                    finally:
                        f.close()
                else:
                    self.request.send("no taskid")
                    print("notaskid")

        #yzh
        elif "getMasterList" in retjson:
            self.request.send(json.dumps(masters).encode())
        elif "getTaskList" in retjson:
            self.request.send(json.dumps(tasklist).encode())
        elif "getSlaveList" in retjson:
            self.request.send(json.dumps(slaves).encode())
        elif "getPendinglist" in retjson:
            self.request.send(json.dumps(pendinglist).encode())
        #yzh
        self.request.close()



#
def getmasters():
    for i in masters:
        print(i+":"+masters[i])

def getslaves():
    for i in slaves:
        print(i+":"+str(slaves[i]))

def getsynclist():
    for i in synclist:
        print(i+":"+str(synclist[i]))

def gettasklist():
    for i in tasklist:
        print(i+":"+str(tasklist[i]))

def getpendinglist():
    for i in pendinglist:
        print(str(i))

def readsave():
    if os.path.exists("supernode.save"):
      fd=None
      try:
        fd=open("supernode.save",r)
        masters=eval(fd.readline())
        slaves=eval(fd.readline())
        tasklist=eval(fd.readline())
        pendinglist=eval(fd.readline())
      except:
        print("read file error")
      finally:
        fd.close()

def getinput():
    while True:
        raw=input(">")
        if raw=="master":
            getmasters()
        if raw=="slave":
            getslaves()
        if raw=="sync":
            getsynclist()
        if raw=="lock":
            print(locklist["sync"])
        if raw=="t":
            gettasklist()
        if raw=="p":
            getpendinglist()
        if raw=="l":
            print(locklist["sync"])
        if raw=="flush":
            flushtofile()
        if raw=="r":
            readsave()

ocmdist={}
ocmdist["server"] = socketserver.ThreadingTCPServer(addr,Servers)
#server.serve_forever()

class dlsocmserver(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        #server = socketserver.ThreadingTCPServer(addr,Servers)
        ocmdist["server"].serve_forever()

if os.path.exists("supernode.save"):
    fd=open("supernode.save","r")
    try:
        masters=eval(fd.readline())
        slaves=eval(fd.readline())
        tasklist=eval(fd.readline())
        pendinglist=eval(fd.readline())
    except:
        print("read file error")
    finally:
        fd.close()
a=dlsocmserver()
a.start()
b=checkstate()
b.start()
c=refreshdoinglist()
c.start()
d=taskrescan()
d.start()
getinput()
