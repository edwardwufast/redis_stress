import os
import sys
import time
import threading
import logging
from datetime import datetime

import redis



class benchmark:

    def __init__(self, record_dir, test_time, redis_commandset, redis_server):
        self.record_dir = record_dir
        self.test_time = test_time
        self.redis_commandset = redis_commandset
        self.redis_server = redis_server 

    def init_client(self):
        pool = redis.ConnectionPool(host=self.redis_server, port=6379, db=0)
        self.client = redis.Redis(connection_pool=pool)

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
    
    def load_script(self, script):
        self.script=script
        self.script_id = self.client.script_load(script)

    def start(self):
        script="redis.call('psetex', 'key', 1000, 'hello')"
        self.load_script(script)
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            self.client.evalsha(self.script_id, 0)


def create_dir():
    if not os.path.exists(RECORD_DIR):
        os.makedirs(RECORD_DIR)

