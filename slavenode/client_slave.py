import socket
import psutil
import subprocess
import time
import os
import subprocess
import paramiko
import hashlib
import threading

os.system("docker rm -f $(docker ps -aq)")
resc = {}
resc['cpu'] = resc['gpu'] = resc['mem'] = resc['disk'] = 0

def total():
    msg={}
    msg["uuid"]=uuid
    msg["cpu"]=psutil.cpu_count()
    msg["gpu"]=1 if subprocess.getstatusoutput('nvidia-smi')[0]==0 else 0
    msg["mem"]=round((psutil.virtual_memory().total)/(1024*1024*1024), 2)
    msg["disk"]=round((psutil.disk_usage('/').available)/(1024*1024*1024), 2)
#    msg["rdisk"]=round((psutil.disk_usage('/').free)/(1024*1024*1024), 2)
#    msg["rmem"]=round((psutil.virtual_memory().available)/(1024*1024*1024), 2)
    
    msg["mem"]=round(msg["mem"]*0.8, 2)

    msg["rdisk"]=float(msg["disk"])-float(resc['disk'])
    msg["rmem"]=float(msg["mem"])-float(resc['mem'])
    msg["rgpu"]=int(msg["gpu"])-int(resc['gpu'])
    msg["rcpu"]=int(msg["cpu"])-int(resc['cpu'])
    return str(msg)

def clients(msg):
    ret=''
    client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    client.settimeout(5)
    try:
        client.connect(addr)
        client.send(msg.encode())
        ret=client.recv(1024).decode()
    except Exception as e:
        print(e)
    finally:
        client.close()
    return ret

class myThread(threading.Thread):
 
    def __init__(self,message):
        threading.Thread.__init__(self)
        ret=eval(message)
        self.taskid=ret.get('_id')
        self.name=ret.get('dockername1')
        self.cmd=ret.get('cmd1')
        self.username=ret.get('username', 'ocloud')
        self.password=ret.get('password', 'ohcloud@123')
        self.remote=ret.get('remote', '18.18.110.220:5000')
        self.runtime=ret.get('renttime', '60')
        self.cpu=int(ret.get('cpu', '1'))
        self.gpu=int(ret.get('gpu', '0'))
        self.mem=float(ret.get('mem', '0'))
        self.disk=float(ret.get('disk', '0'))

    def run(self):
        resc['cpu'] = resc['cpu']+self.cpu
        resc['gpu'] = resc['gpu']+self.gpu
        resc['mem'] = resc['mem']+self.mem
        resc['disk'] = resc['disk']+self.disk
        if self.taskid and self.name and self.cmd:
            ret=self.pull_run(self.name,self.cmd,self.username,self.password,self.remote)
            if ret==0:
                self.plan(self.name,self.runtime)
            else:
                os.system("docker rmi -f %s"%self.name)
                print('run docker images error')
                self.clients("taskfailed{'taskid':'%s','uuid':'%s','dockername1':'%s','renttume':'%s','spendtime':'%s'}"%(self.taskid,uuid,self.name,self.runtime,'0'))
        else:
            self.clients('recv message error')
        print('thread1 finish')
        resc['cpu'] = resc['cpu']-self.cpu
        resc['gpu'] = resc['gpu']-self.gpu
        resc['mem'] = resc['mem']-self.mem
        resc['disk'] = resc['disk']-self.disk


    def pull_run(self,name,cmd,username,password,remote):
        login=subprocess.getoutput('docker login -u %s -p %s %s'%(username,password,remote))
        pull=subprocess.getoutput('docker pull %s'%name)
        logout=subprocess.getoutput('docker logout %s'%remote)
        run=subprocess.getstatusoutput('%s'%cmd)
        print('---\n',login,'\n---\n',pull,'\n---\n',logout,'\n---\n',run[1],'\n---')
        return run[0]

    def plan(self,name,runtime):
        runtime=int(runtime)*60
        spendtime=0
        task = {}
        task['taskid']=self.taskid
        task['uuid']=uuid
        task['dockername1']=name
        task["renttime"]=runtime//60
        while True:
            print(spendtime)
            flag=self.clients('test')
            if not flag:
                print('master dead')
                break
            pid=subprocess.getoutput("docker ps |grep %s |awk '{print $1}'"%name)
            if not pid:
                print('%s exit'%name)
                task['spendtime']=0
                self.clients('taskfailed'+str(task))
                break
            if spendtime!=0 and spendtime%60==0:
                print('%s running %s'%(name,spendtime))
                task['spendtime']=1
                self.clients('taskrunning'+str(task))
            if spendtime==runtime:
                print('%s task finish %s'%(name,spendtime))
                break
            spendtime=spendtime+10
            time.sleep(10)
        pid=subprocess.getoutput("docker ps |grep %s |awk '{print $1}'"%name)
        if pid:os.system("docker rm -f %s"%pid)
        os.system("docker rmi -f %s"%name)

    def clients(self,msg):
        ret=1
        client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        client.settimeout(5)
        try:
            client.connect(addr)
            client.send(msg.encode())
        except:
            ret = 0
        finally:
            client.close
        return ret


if __name__=="__main__":
    uuid='ct'
    #host='116.62.129.207'
    host='18.18.110.220'
    port=12354
    addr=(host,port)
    #addr=('18.18.6.67',20086)
    #while False:
    while True:
        time.sleep(5)
        total_msg = total()
        recv_msg = clients('resource'+total_msg)
        print("Send:",total_msg)
        print("Recv:",recv_msg)
        if not recv_msg:continue
        try:
            eval(recv_msg)
        except:
            pass
        else:
            print('start thread1')
            thread1=myThread(str(recv_msg))
            thread1.start()








