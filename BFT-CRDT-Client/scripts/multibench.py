#!/usr/bin/python3

import csv
import json
from pickle import BUILD, TRUE
from sre_constants import REPEAT
import start_servers
import time
import traceback
import datetime 
import subprocess
import os

# 1. prep json
# set & variables
# fix each variable, change others
# 2. start servres
# 3. run bench
# 4. collect data
# 5. stop servers

BENCHMARK_PATH = "../bin/Release/net8.0/BFT-CRDT-Client"

def run_experiment(workload_config: dict, prime_variable : str, secondary_variable : str, rfilename : str, host_list : list, repeat = 1):

    start = datetime.datetime.now()
    print("Currrent time: " + str(start))
    print("Running: " + rfilename)    
    
    start_servers.build_server()

    # y-axis
    primaries = workload_config[prime_variable]

    # more bars
    secondaries = workload_config[secondary_variable]

    json_dict = workload_config.copy()

    total = len(primaries) * len(secondaries)
    count = 0

    # clear the file
    open("results/" + rfilename + ".txt", "w+").close()

    build_flag = True
    # running benchmarks
    for p in primaries:
        for s in secondaries:
            
            # for repeat
            for redo in range(repeat):
                json_dict[prime_variable] = p
                json_dict[secondary_variable] = s

                wlfilename = str(p) + str(s) + ".json"

                # construct addresses which are ip:port in server_list
                addresses = []
                for ip in host_list:
                    addresses.append(ip + f":{start_servers.START_PORT + 1000}")

                json_dict["addresses"] = addresses

                with open(wlfilename, 'w') as json_file:
                    json.dump(json_dict, json_file)

                try:
                    # start servers
                    start_servers.generate_json(1, host_list)
                    start_servers.run_servers_remote(1, host_list, build_flag)

                    # no longer send after the first time
                    build_flag = False
                    
                    # Waiting here for a long time becasue we need to wait for the servers to 
                    # 1. start
                    # 2. join the cluster (servers waits a while before joining the cluster)
                    # 3. initialize the keyspace
                    time.sleep(10)

                    with open("results/" + rfilename + ".txt", "a") as flog:
                        subprocess.Popen([BENCHMARK_PATH, "2", wlfilename, "y"], stdout=flog, stderr=flog).wait()


                except Exception as e:
                    traceback.print_exc()
                    print("Error, redoing left " + str(redo))

                    if (redo <= 0):
                        print("Error, exiting")
                        exit()

                finally:
                    # waiting to makesure clients are actually finished
                    time.sleep(2)
                    start_servers._stop_server_remote(True, False)
                    os.remove(wlfilename)

                count += 1
                print(str(count) + "/" + str(total) + " done")
                end = datetime.datetime.now()
                print("Elapsed time:" + str(end - start))
                time.sleep(1)

    # create "temp/ip_list_file.txt" again
    for ip in host_list:
        with open("temp/ip_list_file.txt", "a") as f:
            f.write(ip + "\n")

    start_servers._stop_server_remote(True, True)

    print("Experiment complete")
    print("=============================================================")

