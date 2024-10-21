#!/usr/bin/python3

from multibench import *
from start_servers import *

# Insturction:
# 1. Make a copy of this file and rename to run_multi_bench.py
# 2. Change SERVER_LIST to remote servers with RAC server
# 3. see jsons below to define a workload


SERVER_LIST = ['192.168.41.243', '192.168.41.56', '192.168.41.177',  '192.168.41.193']#, 
               #'192.168.41.206', '192.168.41.57', '192.168.41.159', '192.168.41.240']
               #'192.168.41.165', '192.168.41.212', '192.168.41.239', '192.168.41.166',
               #'192.168.41.87', '192.168.41.117', '192.168.41.62', '192.168.41.163']
               # 192.168.41.221, 192.168.41.107, 192.168.41.196, 192.168.41.86, 192.168.41.90']

if __name__ == "__main__":

    for i in [0.5, 1]: 
        t = {
            "clientThreads": 32,
            "typeCode": "orset",
            "numObjs": 100,
            "duration": 30,
            "opsRatio": [[0.5, 0, 0.5]], 
            "safeRatio": i,
            "targetOutputTPS": [1000],
        }

        run_experiment(t, "targetOutputTPS", "opsRatio", "orset-n4-5050RW-" + str(i), SERVER_LIST)

        #run_experiment(t, "targetOutputTPS", "opsRatio", "test", SERVER_LIST)


        

