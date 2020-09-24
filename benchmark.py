import os
import sys
import time
import threading
import logging
import random
import string
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
        self.lua_script = None

    def load_lua_script(self, script_path):
        with open(script_path) as script_fd:
            script = script_fd.read()
        self.lua_script = script

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
        # for cluster mode disable only
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
        self.redis_commandset(self.redis_server, self.client, self.test_time, lua_script=self.lua_script).start()

class commandset:

    def __init__(self, redis_server, client, test_time, lua_script=None):
        self.redis_server = redis_server
        self.client = client
        self.test_time = test_time
        self.lua_script = lua_script
       
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

    def start(self):
        script_id = self.client.script_load(self.lua_script)
        self.client.set("ratelimit_9456909_POST/v1/order/orders/place", 1000)
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            time.sleep(0.1)
            self.client.evalsha(script_id, 0, "ratelimit_9456909_POST/v1/order/orders/place", 200, 1, 2000)

class send_evalsha_no_pool(commandset):

    def start(self):
        script_id = self.client.script_load(self.lua_script)
        self.client.set("ratelimit_9456909_POST/v1/order/orders/place", 1000)
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            time.sleep(0.4)
            execute_low_level(750, 0.04, 'evalsha', script_id, 0, "ratelimit_9456909_POST/v1/order/orders/place", 200, 1, 2000, host=self.redis_server, port=6379)

class send_evalsha_no_pool_v2(commandset):

    def start(self):
        script_id = self.client.script_load(self.lua_script)
        self.client.set("ratelimit_9456909_POST/v1/order/orders/place", 1000)
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            execute_low_level(400, 0.1, 'evalsha', script_id, 0, "ratelimit_9456909_POST/v1/order/orders/place", 200, 1, 2000, host=self.redis_server, port=6379)

class send_set_randomkey(commandset):

    def start(self):
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            random_number = random.random()
            self.client.set('key:' + str(random_number), random_number)

class send_set_randomkey_one_MB(commandset):
    
    def start(self):
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            value = ''.join(random.choice(string.ascii_lowercase) for x in range(1048576))
            key = "key" + value[:10]
            self.client.set(key, value)

class send_set_randomkey_record_failure(commandset):
    
    def start(self):
        timeout_start = time.time()
        while time.time() < timeout_start + self.test_time:
            random_number = random.random()
            print(datetime.utcnow())
            try:
                response = self.client.set('key:' + str(random_number), random_number)
                print(response)
            except Exception as e:
                print(e)
            time.sleep(1)

class cluster_slot_test(commandset):
    
    import dns.resolver

    master_DNS = 'cluster-mode-enable-0002-003.ryi7vr.0001.apne1.cache.amazonaws.com'
    replica_DNS = 'cluster-mode-enable-0002-001.ryi7vr.0001.apne1.cache.amazonaws.com'
    key = 'key:0.5071153217346777'
    log = 'log'

    def init_client(self, host):
        startup_nodes = [{"host": host, "port": "6379"}]
        return RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True, readonly_mode=True)

    def start(self):
        timeout_start = time.time()
        master = self.init_client(self.master_DNS)
        replica = self.init_client(self.replica_DNS)
        
        f = open(self.log, "w+")

        
        while time.time() < timeout_start + self.test_time:
            try:
                current_time = datetime.utcnow()
                f.write(str(current_time) + '\n')
                print(current_time)
                ping_response = replica.ping()
                print(ping_response)
                f.write(str(ping_response) + '\n')
                get_response = replica.get(self.key)
                print(f"replica: {get_response}")
                f.write(f"replica: {get_response}\n")
                try:
                    master_get_response = master.get(self.key)
                    print(f"master: {master_get_response}")
                    f.write(f"master: {master_get_response}\n")
                except Exception as e:
                    print(e)
                    f.write(str(e) + '\n')
                    pass
                cluster_slots = replica.cluster_slots()
                print(cluster_slots)
                f.write(str(cluster_slots) + '\n')
                cluster_nodes = replica.cluster_nodes()
                [ k.pop('slots') for k in cluster_nodes ]
                for node in cluster_nodes:
                    print(node)
                    f.write(str(node) + '\n')
                DNS_master_to_recover = [ ip.to_text() for ip in self.dns.resolver.query(self.master_DNS, 'A')][0]
                print(f"DNS_master_to_recover: {DNS_master_to_recover}")
                f.write(f"DNS_master_to_recover: {DNS_master_to_recover}\n")
                DNS_replica_tobe_master = [ ip.to_text() for ip in self.dns.resolver.query(self.replica_DNS, 'A')][0]
                print(f"DNS_replica_tobe_master: {DNS_replica_tobe_master}")
                f.write(f"DNS_replica_tobe_master: {DNS_replica_tobe_master}\n\n")
                print('\n')
            except Exception as e:
                print(e)
                pass
            time.sleep(1)
        
            
            


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
