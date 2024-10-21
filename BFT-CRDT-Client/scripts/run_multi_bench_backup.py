# ==== batch = 1 ==== 
    
# t1 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.25, 0.25, 0.5],
    #     "safeRatio": [0.3],
    #     "targetOutputTPS": [1000, 10000, 50000, 100000, 200000, 300000, 400000, 500000]
    # }

    # run_experiment(t1, "targetOutputTPS", "safeRatio", "pnc-n4-5050RW-0.3", SERVER_LIST)

    # t2 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.25, 0.25, 0.5],
    #     "safeRatio": [0.5],
    #     "targetOutputTPS": [1000, 10000, 50000, 100000, 200000, 250000, 300000, 350000]
    # }

    # run_experiment(t2, "targetOutputTPS", "safeRatio", "pnc-n4-5050RW-0.5", SERVER_LIST)

    # t3 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.25, 0.25, 0.5],
    #     "safeRatio": [0.7],
    #     "targetOutputTPS": [1000, 10000, 30000, 60000, 100000, 150000, 200000, 250000]
    # }

    # run_experiment(t3, "targetOutputTPS", "safeRatio", "pnc-n4-5050RW-0.7", SERVER_LIST)

    # t4 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.25, 0.25, 0.5],
    #     "safeRatio": [1],
    #     "targetOutputTPS": [1000, 10000, 30000, 50000, 100000, 150000, 200000, 250000]
    # }
    # run_experiment(t4, "targetOutputTPS", "safeRatio", "pnc-n4-5050RW-1", SERVER_LIST)

    # t5 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.15, 0.15, 0.7],
    #     "safeRatio": [0.3],
    #     "targetOutputTPS": [1000, 10000, 50000, 150000, 250000, 350000, 450000, 550000]
    # }

    # run_experiment(t5, "targetOutputTPS", "safeRatio", "pnc-n4-2575RW-0.3", SERVER_LIST)

    # t6 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.15, 0.15, 0.7],
    #     "safeRatio": [0.5],
    #     "targetOutputTPS": [1000, 10000, 50000, 150000, 250000, 300000, 350000, 400000]
    # }

    # run_experiment(t6, "targetOutputTPS", "safeRatio", "pnc-n4-2575RW-0.5", SERVER_LIST)

    # t7 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.15, 0.15, 0.7],
    #     "safeRatio": [0.7],
    #     "targetOutputTPS": [1000, 10000, 50000, 100000, 150000, 200000, 250000, 300000]
    # }

    # run_experiment(t7, "targetOutputTPS", "safeRatio", "pnc-n4-2575RW-0.7", SERVER_LIST)

    # t8 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.15, 0.15, 0.7],
    #     "safeRatio": [1],
    #     "targetOutputTPS": [1000, 10000, 30000, 50000, 100000, 150000, 200000, 250000]
    # }
    # run_experiment(t8, "targetOutputTPS", "safeRatio", "pnc-n4-2575RW-1", SERVER_LIST)

    # t9 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.35, 0.35, 0.3],
    #     "safeRatio": [0.3],
    #     "targetOutputTPS": [1000, 10000, 100000, 200000, 250000, 350000, 400000, 450000]
    # }

    # run_experiment(t9, "targetOutputTPS", "safeRatio", "pnc-n4-7525RW-0.3", SERVER_LIST)

    # t10 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.35, 0.35, 0.3],
    #     "safeRatio": [0.5],
    #     "targetOutputTPS": [1000, 10000, 50000, 100000, 150000, 200000, 250000, 350000]
    # }

    # run_experiment(t10, "targetOutputTPS", "safeRatio", "pnc-n4-7525RW-0.5", SERVER_LIST)

    # t11 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.35, 0.35, 0.3],
    #     "safeRatio": [0.7],
    #     "targetOutputTPS": [1000, 10000, 30000, 50000, 100000, 150000, 200000, 250000]
    # }

    # run_experiment(t11, "targetOutputTPS", "safeRatio", "pnc-n4-7525RW-0.7", SERVER_LIST)

    # t12 = {
    #     "clientThreads": 12,
    #     "typeCode": "pnc",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.35, 0.35, 0.3],
    #     "safeRatio": [1],
    #     "targetOutputTPS": [1000, 10000, 30000, 50000, 70000, 90000, 120000, 150000, 200000]
    # }
    # run_experiment(t12, "targetOutputTPS", "safeRatio", "pnc-n4-7525RW-1", SERVER_LIST)
    







#========= ORSET =========


    # t1 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.25, 0.25, 0.5],
    #     "safeRatio": [0.3],
    #     "targetOutputTPS": [1000, 10000, 40000, 80000, 120000, 160000, 200000, 240000]
    # }

    # run_experiment(t1, "targetOutputTPS", "safeRatio", "orset-n4-5050RW-0.3", SERVER_LIST)

    # t2 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.25, 0.25, 0.5],
    #     "safeRatio": [0.5],
    #     "targetOutputTPS": [1000, 10000, 40000, 80000, 120000, 160000, 200000, 240000]
    # }

    # run_experiment(t2, "targetOutputTPS", "safeRatio", "orset-n4-5050RW-0.5", SERVER_LIST)

    # t3 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.25, 0.25, 0.5],
    #     "safeRatio": [0.7],
    #     "targetOutputTPS": [1000, 5000, 10000, 20000, 40000, 60000, 80000, 10000]
    # }

    # run_experiment(t3, "targetOutputTPS", "safeRatio", "orset-n4-5050RW-0.7", SERVER_LIST)

    # t4 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.25, 0.25, 0.5],
    #     "safeRatio": [1],
    #     "targetOutputTPS": [1000, 5000, 10000, 20000, 40000, 60000, 80000, 10000]
    # }

    # run_experiment(t4, "targetOutputTPS", "safeRatio", "orset-n4-5050RW-1", SERVER_LIST)

    # t5 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.15, 0.1, 0.75],
    #     "safeRatio": [0.3],
    #     "targetOutputTPS": [1000, 10000, 40000, 80000, 120000, 160000, 200000, 240000]
    # }

    # run_experiment(t5, "targetOutputTPS", "safeRatio", "orset-n4-2575RW-0.3", SERVER_LIST)

    # t6 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.15, 0.1, 0.75],
    #     "safeRatio": [0.5],
    #     "targetOutputTPS": [1000, 10000, 40000, 80000, 120000, 160000, 200000, 240000]
    # }

    # run_experiment(t6, "targetOutputTPS", "safeRatio", "orset-n4-2575RW-0.5", SERVER_LIST)

    # t7 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.15, 0.1, 0.75],
    #     "safeRatio": [0.7],
    #     "targetOutputTPS": [1000, 5000, 10000, 20000, 40000, 60000, 80000, 10000]
    # }

    # run_experiment(t7, "targetOutputTPS", "safeRatio", "orset-n4-2575RW-0.7", SERVER_LIST)

    # t8 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.15, 0.1, 0.75],
    #     "safeRatio": [1],
    #     "targetOutputTPS": [1000, 5000, 10000, 20000, 40000, 60000, 80000, 10000]
    # }

    # run_experiment(t8, "targetOutputTPS", "safeRatio", "orset-n4-2575RW-1", SERVER_LIST)

    # t9 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.4, 0.35, 0.25],
    #     "safeRatio": [0.3],
    #     "targetOutputTPS": [1000, 10000, 40000, 80000, 120000, 160000, 200000, 240000]
    # }

    # run_experiment(t9, "targetOutputTPS", "safeRatio", "orset-n4-7525RW-0.3", SERVER_LIST)

    # t10 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.4, 0.35, 0.25],
    #     "safeRatio": [0.5],
    #     "targetOutputTPS": [1000, 10000, 40000, 80000, 120000, 160000, 200000, 240000]
    # }

    # run_experiment(t10, "targetOutputTPS", "safeRatio", "orset-n4-7525RW-0.5", SERVER_LIST)

    # t11 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.4, 0.35, 0.25],
    #     "safeRatio": [0.7],
    #     "targetOutputTPS": [1000, 5000, 10000, 20000, 40000, 60000, 80000, 10000]
    # }

    # run_experiment(t11, "targetOutputTPS", "safeRatio", "orset-n4-7525RW-0.7", SERVER_LIST)

    # t12 = {
    #     "clientThreads": 12,
    #     "typeCode": "orset",
    #     "numObjs": 100,
    #     "duration": 45,
    #     "opsRatio": [0.4, 0.35, 0.25],
    #     "safeRatio": [1],
    #     "targetOutputTPS": [1000, 5000, 10000, 20000, 40000, 60000, 80000, 10000]
    # }

    # run_experiment(t12, "targetOutputTPS", "safeRatio", "orset-n4-7525RW-1", SERVER_LIST)


# ========= batch = 300 =========

