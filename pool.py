import sys
import time
import threading
import logging
from datetime import datetime

import redis

# 'redisconnect.ryi7vr.0001.apne1.cache.amazonaws.com'
# 172.31.31.81

RECORD_DIR = sys.argv[1]
TEST_TIME = 10800
KEY_VALUE = "hello"
#REDIS_SERVER = 'redisconnect.ryi7vr.0001.apne1.cache.amazonaws.com'
REDIS_SERVER = sys.argv[2]
POOL_SIZE = int(sys.argv[3])

def save_info(client, filename):
    with open(RECORD_DIR+"/" + filename, 'w+') as info_file:
        info_result=client.info()
        commandstats_result=client.info(section='commandstats')
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        info_file.write(str(current_time) + "\n\n")
        info_file.write(str(info_result) + "\n\n")
        info_file.write(str(commandstats_result) + "\n\n")

def save_slow(client, filename):
    with open(RECORD_DIR+"/" + filename, 'w+') as slow_file:
        slow_result=client.slowlog_get(num=128)
        slow_file.write(str(slow_result) + "\n\n")


def send_psetex(client):
    timeout_start = time.time()
    while time.time() < timeout_start + TEST_TIME:
        client.psetex('key', 1000, KEY_VALUE)



if __name__ == '__main__':
    pool = redis.ConnectionPool(host=REDIS_SERVER, port=6379, db=0, max_connections=POOL_SIZE)
    client = redis.Redis(connection_pool=pool)
    th = threading.Thread(target=send_psetex, args=(client,))
    save_info(client, 'begion')
    threads = list()
    for index in range(8):
        logging.info("Main    : create and start thread %d.", index)
        x = threading.Thread(target=send_psetex, args=(client,))
        threads.append(x)
        x.start()
    time.sleep(10)
    save_info(client, 'middle')
    for index, thread in enumerate(threads):
        logging.info("Main    : before joining thread %d.", index)
        thread.join()
        logging.info("Main    : thread %d done", index)
    save_info(client, 'end')
    
    save_slow(client, 'slow')    
