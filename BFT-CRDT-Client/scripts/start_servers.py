#! /usr/bin/python3
import pathlib
import sys
import json
import socket
import os
import subprocess
import time
import math
from pathlib import Path

# remote:
# run startservers.py separately on each server (start, stop)
# start server remote use ssh to call startservers.py on all servers


ROOT_PATH = str(Path(__file__).resolve().parent.parent.parent)
PROJECT_PATH = ROOT_PATH + "/BFT-CRDT"
CWD_PATH  = str(Path(__file__).resolve().parent)
START_PORT = 5000

REMOTE_USER_NAME = "ubuntu"
REMOTE_KEY_FILE = "~/bft-crdt.pem"
REMOTE_STORE_PATH = "~/BFT-CRDT"


def each_server_json(node_id: int, servers_per_host: int, host_list: list , selfip: str,  print_addr: bool = False) -> str:
    '''
    Generate the JSON file of a single host and store it in [CWD]/temp directory. 
    '''
    res = []
    addresses = []
    addresses_to_print = []

    i = 0
    for ip in host_list:
        port_offset = 0
        for _ in range(servers_per_host):
            isself = False
            if (i == node_id and ip == selfip):
                isself = True

            cfg = {
                "nodeid": i,
                "address": ip,
                "port": START_PORT + port_offset,
                "isSelf": isself
            }
            res.append(cfg)
            addresses.append(ip + ":" + str(START_PORT + port_offset))
            addresses_to_print.append(ip + ":" + str(START_PORT + port_offset + 1000))
            i += 1
            port_offset += 1

    if print_addr:
        print("Server's client interface addresses:")
        print(addresses_to_print)
        

    return json.dumps(res), addresses


def generate_json(servers_per_host : int, host_list : list) -> list:
    '''
    Generate JSON files for each server instances and store them in [CWD]/temp directory. 
    Return a list of addresses of all servers.
    '''

    # local ip
    selfip = socket.gethostbyname(socket.gethostname())

    if host_list == []:
        host_list = [selfip]

    addresses = []
    i = 0

    for ip in host_list:
        for _ in range(servers_per_host):
            cfg_json, addresses = each_server_json(i, servers_per_host, host_list, ip, i == 0)
            f = open(CWD_PATH + "/temp/cluster_config." + str(i) + ".json", "w")
            f.write(cfg_json)
            f.close()
            i += 1

    return addresses

def build_server():
    '''
    Build a single self-contained executable for the server and stored in [CWD]/temp directory.
    '''
    print("Building...")
    _temp_cleanup()
    subprocess.Popen(
            ["dotnet", "publish", PROJECT_PATH, "-c", "Release", "-r", "linux-x64", "--self-contained", "true", "-f", "net8.0" , "--output", CWD_PATH + "/temp"]).wait()


def start_server(servers_per_host : int, host_list : list = []) -> list:
    '''
    Start [servers_per_host] * [host_list] servers. 
    If servers_list is empty, start servers on local host.
    '''

    build_server()

    addresses = generate_json(servers_per_host, host_list)

    if (host_list == []):
        run_servers_local(servers_per_host)
    else:
        run_servers_remote(servers_per_host, host_list)
    
    return addresses

def run_servers_local(servers_per_host : int):
    pid_list_file = open(CWD_PATH + "/temp/pid_list_file.txt", "w")

    print("Server started at pid:")
    
    for i in range(servers_per_host):
        cfg = CWD_PATH + "/temp/cluster_config." + str(i) + ".json"
        flog = open(CWD_PATH + "/temp/log." + str(i) + ".txt", "w")
        
        proc = subprocess.Popen(
            [CWD_PATH + "/temp/BFT-CRDT", cfg, "1"], stdout=flog, stderr=flog)
        
        pid = str(proc.pid)
        pid_list_file.write(pid + "\n")
        print(pid)
        
        time.sleep(0.1)

    pid_list_file.close()



def run_servers_remote(num_server : int, servers_list : list, build=True):

    temp_file = open(CWD_PATH + "/temp/ip_list_file.txt", "w")

    print("Sending files to remote node")
    for ip in servers_list:
        temp_file.write(ip + "\n")
        if build:
            for _ in range(num_server):
                # send BFT-CRDT and json to remote server
                subprocess.Popen(
                    ["scp", "-i", REMOTE_KEY_FILE, "-r", CWD_PATH + "/temp", REMOTE_USER_NAME + "@" + ip + ":" + REMOTE_STORE_PATH], stdout=subprocess.DEVNULL).wait()
                # change permission
                subprocess.Popen( 
                    ["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, "chmod +x " + REMOTE_STORE_PATH + "/temp"]).wait()

    temp_file.close()
    i = 0
    print("Starting processes")
    for ip in servers_list:
        for _ in range(num_server):
            cfg = REMOTE_STORE_PATH + "/temp/cluster_config." + str(i) + ".json"
            flog = open(CWD_PATH + "/temp/log." + str(i) + ".txt", "w")
            # run BFT-CRDT at remote and output to flog
            subprocess.Popen(["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, REMOTE_STORE_PATH + "/temp/BFT-CRDT", cfg, "1"], stdout=flog, stderr=flog)
            i += 1
    

def stop_server(delete_log: bool):
    # two types of temp_file 
    pid_temp_file = CWD_PATH + "/temp/pid_list_file.txt"
    ip_temp_file = CWD_PATH + "/temp/ip_list_file.txt"

    # check which one exists
    if os.path.exists(pid_temp_file) and os.path.exists(ip_temp_file):
        raise IndentationError("Both local and remote servers are started!")
    
    print("Stopping processes")
    if os.path.exists(pid_temp_file):
        _stop_server(delete_log)
    elif os.path.exists(ip_temp_file):
        _stop_server_remote(delete_log)
    else:
        raise IndentationError("Servers are not started!")


def _stop_server_remote(delete_log: bool, delete_remote:bool = True):
    temp_file = CWD_PATH + "/temp/ip_list_file.txt"

    with open(temp_file, "r") as ftemp:
        line = ftemp.readline()
        while(line):
            ip = line.strip()
            proc = subprocess.run(
                ["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, "killall -SIGINT BFT-CRDT"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            # delete files
            if delete_remote:
                subprocess.run(["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, "rm -rf " + REMOTE_STORE_PATH + "/temp"])
            
            line = ftemp.readline()

            

    # just in case
    with open(temp_file, "r") as ftemp:
        line = ftemp.readline()
        while(line):
            ip = line.strip()
            proc = subprocess.run(["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, "killall BFT-CRDT"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            line = ftemp.readline()

    if delete_log:
        _temp_cleanup()


def _stop_server(delete_log: bool):
    # delete json files
    import signal
    
    temp_file = CWD_PATH + "/temp/pid_list_file.txt"

    with open(temp_file, "r") as ftemp:
        pid = int(ftemp.readline())
        print("Server stopped with pid:")
        while(pid):
            print(pid)
            try:
                os.kill(pid, signal.SIGINT)
                print(pid)
            except OSError:
                continue
            finally:
                try:
                    pid = int(ftemp.readline())
                except ValueError:
                    break
        
    # duh
    subprocess.run(["killall", "BFT-CRDT"])        

    if delete_log:
        _temp_cleanup()


def restart_server():
    # two types of temp_file 
    pid_temp_file = CWD_PATH + "/temp/pid_list_file.txt"
    ip_temp_file = CWD_PATH + "/temp/ip_list_file.txt"

    # check which one exists
    if os.path.exists(pid_temp_file) and os.path.exists(ip_temp_file):
        raise IndentationError("Both local and remote servers are started!")
    
    if os.path.exists(pid_temp_file):
        # get number of servers in pid_temp_file
        with open(pid_temp_file, "r") as ftemp:
            num_server = len(ftemp.readlines())

        _stop_server(True)
        start_server(num_server)
    elif os.path.exists(ip_temp_file):
        # get ip list
        ip_list = []
        with open(ip_temp_file, "r") as ftemp:
            line = ftemp.readline()
            while(line):
                ip_list.append(line.strip())
                line = ftemp.readline()

        _stop_server_remote(True)
        start_server(1, ip_list)
    else:
        raise IndentationError("Servers are not started!")


def _temp_cleanup():
    '''
    Delete everything in CWD + "/temp" directory
    '''
    temp_dir = os.path.join(os.getcwd(), "temp")
    for file in os.listdir(temp_dir):
        file_path = os.path.join(temp_dir, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)



if __name__ == "__main__":
    try:
        action = sys.argv[1]
    except Exception:
        raise ValueError(
            'Wrong action, Usage: StartServers.py [start/stop/restart/rstart] [number_of_servers]')

    if (action == "start"):
        try:
            num_server = int(sys.argv[2])
        except Exception:
            raise ValueError(
                'Need number of server, Usage: StartServers.py start [number_of_servers]')
        start_server(num_server)

    elif (action == "rstart"):
        try:
            num_pre_server = int(sys.argv[2])
            host_ips = sys.argv[3].split(',')
        except Exception:
            raise ValueError(
                'Need number of server, Usage: StartServers.py rstart [number_pre_servers] [ip1,ip2,...]')
        start_server(num_pre_server, host_ips)

    elif (action == "stop"):
        try:
            if sys.argv[2] == "0":
                delete_log = False
            else:
                delete_log = True
        except Exception:
            raise ValueError(
                'Usage: StartServers.py stop [0: not delete log; 1: delete log]')
        stop_server(delete_log)
    
    elif (action == "restart"):
        restart_server()
    elif (action == "build"):
        build_server()
    else:
        raise ValueError(
            'Wrong action, Usage: StartServers.py [start/stop/restart] [number_of_servers]')