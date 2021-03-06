#from pymongo import MongoClient
from socketserver import StreamRequestHandler as SRH
import threading
import socketserver
import socket
import time
import subprocess
import json
import os
import uuid
import sys
#mongouri='mongodb://127.0.0.1:27017/ocbc'
#a=MongoClient(mongouri)
#b=a.ocbc
#c=b.ocbc
#ret=c.find()
#for i in ret:
#    print(i)

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
                print(taskrelist_m)
                for j in taskrelist_m:
                    for x in slaves:
                        if slaves[x]["master"]==j:
                            print(slaves[x]["task"])
                            #for y in slaves[x]["task"]:
                            for i in slaves[x]["task"]:
                                if slaves[x]["task"][i]["remaintime"]>0:
                                    print("pendinglist+taskrescan_m")
                                    pendinglist.append({"remaintime":slaves[x]["task"][i]["remaintime"],"taskid":slaves[x]["task"][i]["taskid"]})
                                    taskrelist.remove(x)

                print(taskrelist)
                for kk in taskrelist:
                    #for x in slaves[kk]["task"]:
                    for i in slaves[kk]["task"]:
                        if slaves[kk]["task"][i]["remaintime"]>0:
                            print("pendinglist+taskrescan")
                            pendinglist.append({"remaintime":slaves[x]["task"][i]["remaintime"],"taskid":slaves[x]["task"][i]["taskid"]})
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



class taskrescan(threading.Thread): #find proper slavenode and generate subtask
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
                    if "subtaskid" not in j:
                        for ii in slaves:
                            if ii not in slavesdoing:
                                slavesdoing[ii]=0
                            if slavesdoing[ii]==0:
                                if slaves[ii]["rcpu"]>=tasklist[j["taskid"]]["cpu"]:
                                    if slaves[ii]["rmem"]>=tasklist[j["taskid"]]["mem"]:
                                        if slaves[ii]["rgpu"]>=tasklist[j["taskid"]]["gpu"]:
                                            if slaves[ii]["rdisk"]>=tasklist[j["taskid"]]["disk"]:
                                                slavesdoing[ii]=1
                                                j["subtaskid"]=str(uuid.uuid1())
                                                j["slaveid"]=ii
                                                j["master"]=slaves[ii]["master"]
                                                break

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
                pendinglist.append({"remaintime":retjson["renttime"],"taskid":retjson["taskid"]})
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
            #print(retjson)
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
                        output=subprocess.getoutput("md5sum /var/www/html/dist/"+retjson["dockername"]+" |awk '{printf $1}'")
                        retjson["filemd5"]=output
                        tasklist[retjson["taskid"]]=retjson
                        tasklist[retjson["taskid"]]["alltime"]=retjson["renttime"]*retjson["num"]
                        ret=str(launchtask(retjson))
                        self.request.send(ret.encode())
            elif retjson["event"]=="taskprocess":
                for retjson["slaveid"] in slaves:
                    slaves[retjson["slaveid"]]["process"]=retjson["process"]
            elif retjson["event"]=="mastersync":
                #try:
                    while True:
                        if locklist["sync"]:
                            time.sleep(1)
                        else:
                            locklist["sync"]=True
                            break
                    #print("startsync")
                    masters[retjson["masternode"]["name"]]=self.client_address[0]
                    #start sync master
                    synclist_m[retjson["masternode"]["name"]]=0
                    #start sync slaves
                    #print(retjson["slave"])
                    for i in retjson["slave"]:
                        #i=eval(j)
                        #print(i["_id"])
                        synclist[i["_id"]]=0

                        #sync vales except task
                        if i["_id"] not in slaves:
                            slaves[i["_id"]]={}
                            slaves[i["_id"]]["task"]={}
                        slaves[i["_id"]]["cpu"]=i["cpu"]
                        slaves[i["_id"]]["gpu"]=i["gpu"]
                        slaves[i["_id"]]["mem"]=i["mem"]
                        slaves[i["_id"]]["time"]=i["time"]
                        slaves[i["_id"]]["master"]=retjson["masternode"]["name"]
                        # new
                        slaves[i["_id"]]["disk"]=i["disk"]
                        slaves[i["_id"]]["rcpu"]=i["rcpu"]
                        slaves[i["_id"]]["rgpu"]=i["rgpu"]
                        slaves[i["_id"]]["rmem"]=i["rmem"]
                        slaves[i["_id"]]["rdisk"]=i["rdisk"]

                        if i["status"]>0:
                            slaves[i["_id"]]["taskstate"]="working"
                        else:
                            slaves[i["_id"]]["taskstate"]="avaliable"
                        #print("starttasksync")
                        #start sync (sub)task
                        taskinreal=i["task"]
                        taskindb=slaves[i["_id"]]["task"]
                        dellist=[]
                        for x in taskinreal: #(sub)tasks need to update
                            print(taskindb)
                            if x in taskindb:
                                remaintime=taskindb[x]["remaintime"]-taskinreal[x]["spendtime"]
                                taskremaintime=tasklist[taskindb[x]["taskid"]]["alltime"]-taskinreal[x]["spendtime"]
                                if remaintime<=0:
                                    print(x+":finish")
                                if taskremaintime<=0:
                                    print(taskindb[x]["taskid"]+":finish")
                                slaves[i["_id"]]["task"][x]["remaintime"]=remaintime
                                tasklist[taskindb[x]["taskid"]]["alltime"]=taskremaintime
                            else: #strange (sub)task
                                #if taskinreal[x]["taskid"] in tasklist:
                                #    print("warning:slavenode report new task-"+taskinreal[x]["taskid"])
                                #    taskindb[x]={"taskid":taskinreal[x]["taskid"],"remaintime":?}
                                print("ERROR:"+x+" is a strange subtask")
                        for y in taskindb: #(sub)tasks need to delete
                            if y not in taskinreal:
                                if taskindb[y]["remaintime"]>0:
                                    print("pendinglist+mastersync")
                                    pendinglist.append({"taskid":taskindb[y]["taskid"],"remaintime":taskindb[y]["remaintime"]})
                                dellist.append(y)

                        for k in dellist:#delete (sub)task
                            del slaves[i["_id"]]["task"][k]

                    #print("startsubtasksync")
                    #master get (sub)task
                    #launchtask=[]
                    delplist=[]
                    for p in pendinglist:
                        if "master" in p:
                          if p["master"]==retjson["masternode"]["name"]:
                            if p["slaveid"] in slaves:
                                print("getit")
                                temp={}
                                temp["cmd"]=tasklist[p["taskid"]]["cmd"]
                                temp["dockername"]=tasklist[p["taskid"]]["dockername"]
                                temp["slaveid"]=p["slaveid"]
                                temp["renttime"]=p["remaintime"]
                                temp["taskid"]=p["taskid"]
                                temp["filemd5"]=tasklist[p["taskid"]]["filemd5"]
                                temp["subtaskid"]=p["subtaskid"]
                                temp["cpu"]=tasklist[p["taskid"]]["cpu"]
                                temp["mem"]=tasklist[p["taskid"]]["mem"]
                                temp["gpu"]=tasklist[p["taskid"]]["gpu"]
                                temp["disk"]=tasklist[p["taskid"]]["disk"]
                                #print("launchtask"+str(temp))
                                self.request.send(("launchtask"+str(temp)).encode())
                                ret=self.request.recv(64)
                                ret1=ret.decode()
                                if ret1=="launchsuccess":
                                    print("received launchsuccess"+str(p))
                                    slaves[p["slaveid"]]["task"][p["subtaskid"]]={"taskid":p["taskid"],"remaintime":p["remaintime"]}
                                    delplist.append(p)
                                else:#if launch fail
                                    pass
                            else:
                                del p["slaveid"]
                                del p["master"]
                                del p["subtaskid"]
                    self.request.send("nopendingtask".encode())
                    for o in delplist:
                        print(str(o)+"deleted from pendinglist")
                        pendinglist.remove(o)

                #except Exception as e:
                #   print("whaterror")
                #finally:
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

ocmdist={}
ocmdist["server"] = socketserver.ThreadingTCPServer(addr,Servers)


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
        if raw=="es":
            ocmdist["server"].shutdown()
            sys.exit(0)
#ocmdist={}
#ocmdist["server"] = socketserver.ThreadingTCPServer(addr,Servers)
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
