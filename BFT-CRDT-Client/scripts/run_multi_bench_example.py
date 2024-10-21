#!/usr/bin/python3

from multibench import *
from start_servers import *

# Insturction:
# 1. Make a copy of this file and rename to run_multi_bench.py
# 2. Change SERVER_LIST to remote servers with RAC server
# 3. see jsons below to define a workload


SERVER_LIST = ['192.168.41.42', '192.168.41.162', '192.168.41.204', '192.168.41.17']# , '192.168.41.227', '192.168.41.10', '192.168.41.52', '192.168.41.20']#, '192.168.41.195', '192.168.41.193', '192.168.41.93', '192.168.41.188', '192.168.41.95', '192.168.41.21', '192.168.41.12', '192.168.41.121']

if __name__ == "__main__":
    # test = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.35, 0.35, 0.3],
    #     "safeRatio": [0.5],
    #     "targetOutputTPS": [200000, 250000, 300000]#[1000, 10000, 50000, 100000, 150000, 200000]
    # }

    # run_experiment(test, "targetOutputTPS", "safeRatio", "test_result_file", SERVER_LIST)

    single = {
        "clientThreads": 12,
        "typeCode": "orset",
        "numObjs": 100,
        "duration": 20,
        "opsRatio": [0.5, 0, 0.5],
        "safeRatio": [0.5],
        "targetOutputTPS": [100000],
    }

    run_experiment(single, "targetOutputTPS", "safeRatio", "single", SERVER_LIST)
    exit(0)

    t1 = {
        "clientThreads": 12,
        "typeCode": "orset",
        "numObjs": 100,
        "duration": 25,
        "opsRatio": [0.5, 0, 0.5],
        "safeRatio": [0],
        "targetOutputTPS": [150000, 200000, 250000, 300000]
    }

    #run_experiment(t1, "targetOutputTPS", "safeRatio", "nodag3", SERVER_LIST)

    t2 = {
        "clientThreads": 12,
        "typeCode": "orset",
        "numObjs": 100,
        "duration": 45,
        "opsRatio": [0.3, 0, 0.7],
        "safeRatio": [0.5],
        "targetOutputTPS": [100000]
    
    }
    #run_experiment(t2, "targetOutputTPS", "safeRatio", "orset-375-tpvslt-1500", SERVER_LIST)

    t3 = {
        "clientThreads": 12,
        "typeCode": "orset",
        "numObjs": 100,
        "duration": 45,
        "opsRatio": [0.7, 0, 0.3],
        "safeRatio": [0.5],
        "targetOutputTPS": [80000]
    }

    #run_experiment(t3, "targetOutputTPS", "safeRatio", "orset-735-tpvslt-1500", SERVER_LIST)


    # t2 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.25, 0.25, 0.5],
    #     "safeRatio": [0],
    #     "targetOutputTPS": [20000, 40000, 60000, 80000, 100000, 120000, 140000, 160000, 180000, 200000, 220000, 240000, 260000, 280000, 300000, 320000, 340000, 360000, 380000, 400000]
    # }

    # run_experiment(t2, "targetOutputTPS", "safeRatio", "pnc-55-nodag", SERVER_LIST)
