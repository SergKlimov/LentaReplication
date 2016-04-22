#!/usr/bin/env python3

import requests
import time
import random
import sys
import multiprocessing

def push(process_number):
    print("Process {} started".format(process_number))
    time.sleep(random.random())

    for x in range(iterations):
        start = time.perf_counter()
        num = process_number*process_count+x
        # print("Process {} pushes {}".format(process_number, num))
        payload = {"number": num,
                   "status": "JSON" + str(num)}
        r = requests.post("http://127.0.0.1:4567/push", json=payload)
        finish = time.perf_counter()
        time.sleep(0.1)
        # time.sleep(1 - (finish - start))


process_count = 300
iterations = process_count

for x in range(process_count):
    p = multiprocessing.Process(target=push, args=(x,))
    p.start()
