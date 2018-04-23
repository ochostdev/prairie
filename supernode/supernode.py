#from pymongo import MongoClient
from socketserver import StreamRequestHandler as SRH
import threading
import socketserver
import socket
import time
import subprocess
import json
#mongouri='mongodb://127.0.0.1:27017/ocbc'
#a=MongoClient(mongouri)
#b=a.ocbc
#c=b.ocbc
#ret=c.find()
#for i in ret:
#    print(i)

addr=("18.18.25.29",18889)

masters={}#
slaves={}
slavesdoing={}
taskrescanlist={}
tasklist={}
synclist={}
synclist_m={}
locklist={}
locklist["sync"]=False
locklist["syncm"]=False
pendinglist=[]


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
                                    pendinglist.append({"renttime":slaves[x]["renttime"],"taskid":slaves[x]["requesttask"]})
                                    taskrelist.remove(x)
                #print(taskrelist) 
                for kk in taskrelist:
                    #for x in slaves[kk]["task"]:
                    if slaves[kk]["requesttask"]!="":
                        if slaves[kk]["renttime"]>0:
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
    #tasks={}
    #taskflag=True
    #for i in range(0,retjson["num"]):
    #    tasks[i]=0
    #while taskflag:
        try:
            while True:
                if locklist["sync"]:
                    time.sleep(1)
                else:
                    locklist["sync"]=True
                    break
            for i in range(0,retjson["num"]):
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
            if retjson["event"]=="launchtask":
            #basic check
                if "cpu" in retjson and "mem" in retjson and "gpu" in retjson:
                    #update mongodb
                    retjson["dep"]="no"
                    if retjson["taskid"] in tasklist:
                        print("dump taskid")
                    else:
                        output=subprocess.getoutput("md5sum /var/www/html/dist/"+retjson["taskid"]+".tar |awk '{printf $1}'")
                        retjson["filemd5"]=output
                        tasklist[retjson["taskid"]]=retjson
                        tasklist[retjson["taskid"]]["alltime"]=retjson["renttime"]*retjson["num"]
                        ret=str(launchtask(retjson))
                        self.request.send(ret.encode())

            elif retjson["event"]=="mastersync":
                try:
                    while True:
                        if locklist["sync"]:
                            time.sleep(1)
                        else:
                            locklist["sync"]=True
                            break
                    masters[retjson["masternode"]["name"]]=self.client_address[0]
                    #start sync master
                    synclist_m[retjson["masternode"]["name"]]=0
                    #start sync slaves
                    #print(retjson["slave"])
                    for i in retjson["slave"]:
                        #i=eval(j)
                        #print(i["_id"])
                        synclist[i["_id"]]=0
                        if i["_id"] not in slaves:
                            slaves[i["_id"]]={}
                            slaves[i["_id"]]["renttime"]=0
                            slaves[i["_id"]]["task"]=[]
                            slaves[i["_id"]]["spendtime"]=0
                            slaves[i["_id"]]["requesttask"]=""
                        slaves[i["_id"]]["cpu"]=i["cpu"]
                        slaves[i["_id"]]["gpu"]=i["gpu"]
                        slaves[i["_id"]]["mem"]=i["mem"]
                        slaves[i["_id"]]["time"]=i["time"]
                        slaves[i["_id"]]["master"]=retjson["masternode"]["name"]

                        if i["status"]>0:
                            slaves[i["_id"]]["taskstate"]="working"
                        else:
                            slaves[i["_id"]]["taskstate"]="avaliable"
                        taskdet=i["task"]
                        if taskdet==[] and slaves[i["_id"]]["renttime"]>0 and slaves[i["_id"]]["task"]!=[] and slaves[i["_id"]]["requesttask"]!="":
                            pendinglist.append({"taskid":slaves[i["_id"]]["requesttask"],"renttime":slaves[i["_id"]]["renttime"]})
                            slaves[i["_id"]]["spendtime"]=0 
                        slavetask=[]
                        for x in taskdet:
                            if "_id" in x:
                                slavetask.append(x["taskid"])
                                slaves[i["_id"]]["renttime"]=slaves[i["_id"]]["renttime"]-x["spendtime"]
                                tasklist[x["taskid"]]["alltime"]=tasklist[x["taskid"]]["alltime"]-x["spendtime"]
                                if slaves[i["_id"]]["renttime"]<=0:
                                    print(i["_id"]+":finishtime")
                        slaves[i["_id"]]["task"]=slavetask
                            #slaves[i["_id"]]["taskstate"]="avaliable"
                        #slavesdoing[i["_id"]]=0
                except Exception as e:
                        print("whaterror")
                finally:
                        locklist["sync"]=False
            elif retjson["event"]=="needfile":
                if "taskid" in retjson:
                    f = open("/var/www/html/dist/"+retjson["taskid"]+".tar",'rb')
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
ocmdist={}
ocmdist["server"] = socketserver.ThreadingTCPServer(addr,Servers)
#server.serve_forever()

class dlsocmserver(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    
    def run(self):
        #server = socketserver.ThreadingTCPServer(addr,Servers)
        ocmdist["server"].serve_forever()


a=dlsocmserver()
a.start()
b=checkstate()
b.start()
c=refreshdoinglist()
c.start()
d=taskrescan()
d.start()
getinput()

