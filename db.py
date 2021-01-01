import pickledb 
import json
import time
import os
import threading

ttl = {}
start_time = {}

def check_ttl(key):
    keys = time_db.getall()
    if key in keys:
        t,s = time_db.lgetall(key)
        if t <= time.time()-s:
            return False

    return True


def insert_data():
    N = int(input("Number of queries to be inserted: "))
    keys = db.getall()
    for i in range(N):
        key = input("Enter Key: ")
        if key in keys:
            print("Key already present insert value only? ")
            choice = input("DO YOU WANT TO INSERT VALUE?(Y/N): ")
            if choice == "Y" or choice == "y":
                for j in keys:
                    val = input("Enter values for {0}: ".format(j))
                    if len(val) > 32:
                        print("Exceeded limit of 32 chars")
                        break
                    db.ladd(j,val)
        elif key not in keys:
            t = int(input("Enter time to live for key: 0 for not ttl "))
            if t>0:
                time_db.lcreate(key)
                time_db.ladd(key,t)
                time_db.ladd(key,time.time())
                time_db.dump()
            if len(keys) > 0:
                for i in keys:
                    x = i
                    n = len(db.lgetall(x))
                    break
                print("There is already a key with more values than the new key please insert the new values for the corresponding values of the previous keys insert 0 if not known")
                db.lcreate(key)
                for j in range(n):
                    print("Enter value for {0}".format(db.lgetall(x)[j]),end="  ")
                    val = input()
                    if len(val) > 32:
                        print("Exceeded limit of 32 chars")
                        break
                    db.ladd(key,val)
            else:
                db.lcreate(key)
                print("Enter value:")
                val = input()
                db.ladd(key,val)
    db.dump()

def delete():
    keys = db.getall()
    print("FOllowing keys are available")
    print([i for i in keys])
    key = input("Please input the key: ")
    res = check_ttl(key)
    if res == False:
        print("TTL expired")
        return
    if key not in keys:
        print("Error key not found")
    else:
        vals = db.lgetall(key)
        print(vals)
        val = input("Enter Value to be deleted:")
        index = vals.index(val)
        for k in keys:
            db.lpop(k,index)
        print("Deleted Sucesfully")
    db.dump()
        
    db.dump()

def read():
    print("Process ID {0} Current Thread {1}".format(os.getpid(),threading.current_thread().name))
    keys = db.getall()
    print("Keys: {0}",format(keys))
    key = input("select a key: ")
    res = check_ttl(key)
    if res == False:
        print("TTL expired")
        return
    if key not in keys:
        print("Key not Found")
        return
    vals = db.lgetall(key)
    j_son = json.dumps((key,vals))
    print(j_son) 

def one():

    print("Process ID {0} Current Thread {1}".format(os.getpid(),threading.current_thread().name))
    lock.acquire()
    if os.stat(name).st_size > 1073741824:
        exit()
    ch = input("1: Insert Data \n2: Delete Data \n3: Read Data\n")
    if ch == "1":
        insert_data()
    elif ch == "2":
        delete()
    elif ch =="3":
        read()
    else:
        exit()
    lock.release()

if __name__ == "__main__":

    name = input("Input file name, if file is not present a new one will be created: ")+".db"
    db = pickledb.load(name,False)
    db.dump()

    time_db = pickledb.load("time.db",False)
    time_db.dump()

    lock = threading.Lock()
    threads = []

    n = int(input("Enter number of threads: "))
    for i in range(n):
        threads.append(threading.Thread(target=one,name="Thread {0}".format(i+1)))

    for t in threads:
        t.start()

    for t in threads:
        t.join()








    

    