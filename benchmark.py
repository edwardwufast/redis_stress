import os
import sys
import time
import threading
import logging
import random
from datetime import datetime

import redis
from rediscluster import RedisCluster



class benchmark:

    def __init__(self, record_dir, test_time, redis_commandset, redis_server):
        self.record_dir = record_dir
        self.test_time = test_time
        self.redis_commandset = redis_commandset
        self.redis_server = redis_server 

    def init_client(self):
        pool = redis.ConnectionPool(host=self.redis_server, port=6379, db=0)
        self.client = redis.Redis(connection_pool=pool)

    def init_cluster_client(self):
        startup_nodes = [{"host": self.redis_server, "port": "6379"}]
        self.client = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)


    def save_info(self, filename):
        with open(self.record_dir + "/" + filename, 'w+') as info_file:
            info_result=self.client.info()
            commandstats_result=self.client.info(section='commandstats')
            now = datetime.now()
            current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            info_file.write(str(current_time) + "\n\n")
            info_file.write('server: ' + str(self.redis_server) + "\n\n")
            info_file.write(str(info_result) + "\n\n")
            info_file.write(str(commandstats_result) + "\n\n")

    def save_slow(self, filename):
        with open(self.record_dir+"/" + filename, 'w+') as slow_file:
            slow_result=self.client.slowlog_get(num=128)
            slow_file.write(str(slow_result) + "\n\n")

    def run(self):
        self.redis_commandset(self.client, self.test_time).start()

class commandset:

    def __init__(self, client, test_time):
        self.client = client
        self.test_time = test_time

class send_psetex(commandset):

    def start(self):
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            self.client.psetex('key', 1000, 'hello')
    
class send_evalsha(commandset):
    script=''    
    script_id=''
    def load_script(script):
        send_evalsha.script = script

    def start(self):
        send_evalsha.script_id = self.client.script_load(send_evalsha.script)
        self.client.set("ratelimit_9456909_POST/v1/order/orders/place", 1000)
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            self.client.evalsha(send_evalsha.script_id, 0, "ratelimit_9456909_POST/v1/order/orders/place", 200, 1, 2000)

class send_set_randomkey(commandset):

    def start(self):
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            random_number = random.random()
            self.client.set('key:' + str(random_number), random_number)

def create_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

