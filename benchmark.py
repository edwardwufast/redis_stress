import os
import sys
import time
import threading
import logging
import random
from datetime import datetime

import redis
import pandas as pd
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
        main_path = self.record_dir + "/" + filename
        with open(main_path, 'w+') as info_file:
            # save current timestamp 
            now = datetime.now()
            current_time = now.strftime("%Y-%m-%d %H:%M:%S")
            info_file.write(str(current_time) + "\n\n")
            # get info commandstats
            info_result = self.client.info()
            info_result.pop('db0')
            info_df = pd.DataFrame(info_result, index={"value"})
            commandstats_result = self.client.info(section='commandstats')
            commandstats_df = pd.DataFrame(commandstats_result)

            # write excel
            info_df.to_excel(main_path + '_info.xlsx')
            commandstats_df.to_excel(main_path + '_commandstats.xlsx')

    def get_time(self):
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        return current_time
    

    def get_info(self):
        info_result = self.client.info()
        info_result.pop('db0')
        info_df = pd.DataFrame(info_result, index={"value"}).transpose()
        return info_df

    def get_commandstats(self):
        commandstats_result = self.client.info(section='commandstats')
        commandstats_df = pd.DataFrame(commandstats_result).transpose()
        return commandstats_df

    def get_slow(self):
        slow_result = self.client.slowlog_get(num=1000)
        for slowlog in slow_result:
             slowlog['start_time']= datetime.utcfromtimestamp(int(slowlog['start_time'])).strftime('%Y-%m-%d %H:%M:%S UTC')
        slowlog_df = pd.DataFrame(slow_result)
        return slowlog_df

    def save_slow(self, filename):
        with open(self.record_dir+"/" + filename, 'w+') as slow_file:
            slow_result=self.client.slowlog_get()
            for slowlog in slow_result:
                slowlog['start_time']= datetime.utcfromtimestamp(int(slowlog['start_time'])).strftime('%Y-%m-%d %H:%M:%S UTC')
            slow_file.write(str(slow_result) + "\n\n")
            slow_file.write("Total: " + str(len(slow_result)))

    def reset_slow(self):
        self.client.slowlog_reset()

    def run(self):
        self.redis_commandset(self.redis_server, self.client, self.test_time).start()

class commandset:

    def __init__(self, redis_server, client, test_time):
        self.redis_server = redis_server
        self.client = client
        self.test_time = test_time

class send_psetex(commandset):

    def start(self):
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            self.client.psetex('key', 1000, 'hello')

class send_psetex_no_pool(commandset): 
    
    def start(self):
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            time.sleep(0.4)
            execute_low_level(750, 0.1, 'psetex', 'key', 1000, 'hello', host=self.redis_server, port=6379)

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
            time.sleep(0.1)
            self.client.evalsha(send_evalsha.script_id, 0, "ratelimit_9456909_POST/v1/order/orders/place", 200, 1, 2000)

class send_evalsha_no_pool(commandset):
    script=''
    script_id=''
    def load_script(script):
        send_evalsha.script = script

    def start(self):
        send_evalsha.script_id = self.client.script_load(send_evalsha.script)
        self.client.set("ratelimit_9456909_POST/v1/order/orders/place", 1000)
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            time.sleep(0.4)
            execute_low_level(750, 0.04, 'evalsha', send_evalsha.script_id, 0, "ratelimit_9456909_POST/v1/order/orders/place", 200, 1, 2000, host=self.redis_server, port=6379)


class send_set_randomkey(commandset):

    def start(self):
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            random_number = random.random()
            self.client.set('key:' + str(random_number), random_number)

def create_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def multiple_dfs(df_list, sheets, file_name, spaces):
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
    col = 0
    for dataframe in df_list:
        dataframe.to_excel(writer,sheet_name=sheets,startrow=0, startcol=col)   
        col = col + len(dataframe.columns) + spaces + 1
    writer.save()

def execute_low_level(loop, sleep_sec, command, *args, **kwargs):
    connection = redis.Connection(**kwargs)
    try:
        connection.connect()
        for i in range(loop):
            time.sleep(sleep_sec)
            connection.send_command(command, *args)

            response = connection.read_response()
            if command in redis.Redis.RESPONSE_CALLBACKS:
                return redis.Redis.RESPONSE_CALLBACKS[command](response)

    finally:
        connection.send_command('quit')
        del connection
