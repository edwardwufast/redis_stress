#!/usr/bin/env python3

import time
import argparse
import threading
import multiprocessing
from datetime import datetime
from statistics import mean 

import pandas as pd

import benchmark
from benchmark import create_dir, multiple_dfs, execute_low_level
from awsapi import get_metric_average

benchmark_dict = {cls.__name__:cls for cls in benchmark.commandset.__subclasses__()}

parser = argparse.ArgumentParser(description='Redis benchmark')

parser.add_argument('-s', metavar='redis_server_IP', required=True)
parser.add_argument('-c', metavar='connection', type=int, required=True)
parser.add_argument('-d', metavar='record_directory', required=True)
parser.add_argument('-t', metavar='test_time', type=int, required=True)
parser.add_argument('-b', metavar='benchmark', choices=[key for key in benchmark_dict], required=True, help=f"Avalible benchmark: {[key for key in benchmark_dict]}")
parser.add_argument('-C', help='cluster mode', default=False, action='store_true', required=False)
parser.add_argument('-l', metavar='lua script file path for send_evalsha', required=False)
parser.add_argument('-n', metavar='excel tab name and file name', required=True)

class StoreDictKeyPair(argparse.Action):
     def __call__(self, parser, namespace, values, option_string=None):
         my_dict = {}
         for kv in values.split(","):
             k,v = kv.split("=")
             my_dict[k] = v
         setattr(namespace, self.dest, my_dict)

parser.add_argument("--key_pairs", dest="my_dict", action=StoreDictKeyPair, metavar="KEY1=VAL1,KEY2=VAL2...")

args = parser.parse_args()


if __name__ == '__main__':
    server = args.s
    connection = args.c
    record_directory = args.d
    test_time = args.t
    benchmark_test = benchmark_dict[args.b]
    cluster_mode_enable = args.C
    script_path = args.l
    tab_name = args.n
    other_args_dict = args.my_dict

    create_dir(record_directory)
    bh = benchmark.benchmark(record_directory, test_time, benchmark_test, server, other_args_dict=other_args_dict)
    if cluster_mode_enable:
        bh.init_cluster_client()
    else:
        bh.init_client()
    if script_path:
        bh.load_lua_script(script_path)
    #bh.reset_slow()
    start_time = bh.get_time()
    commandstats_df_begin = bh.get_commandstats()
    threads = list()
    for index in range(connection):
        x = threading.Thread(target=bh.run)
        threads.append(x)
        x.start()

#    if not cluster_mode_enable:
#        time.sleep(10)
#        info_df_middle = bh.get_info()
#   if cluster_mode_enable:
#        time.sleep(10)
#        info_df_middle = bh.get_info_cluster()
    for index, thread in enumerate(threads):
        thread.join()
    if True:
        end_time = bh.get_time()
        start_time_datetime = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_time_datetime = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        try:
            wanted_cloudwatch_metrics = ['EngineCPUUtilization', 'NetworkBytesIn', 'NetworkBytesOut', 'NewConnections', 'CurrConnections']
            cloudwatch_metrics = [get_metric_average(server.split('.')[0], metric, start_time_datetime, end_time_datetime) for metric in wanted_cloudwatch_metrics]
            cloudwatch_metrics[0]['Label']
            cloudwatch_metrics_avg = {metric['Label']:mean([data['Average'] for data in metric['Datapoints']]) for metric in cloudwatch_metrics}
            system_metrics_df = pd.DataFrame(data=cloudwatch_metrics_avg, index={0}).T
        except Exception as error:
            system_metrics_df = pd.DataFrame(data=[error], index={0}).T
        time_df = pd.DataFrame([start_time, end_time, test_time], index=['start_time', 'end_time', 'test_time'])
        commandstats_df_end = bh.get_commandstats()
        if cluster_mode_enable:
            def f(x, y):
                if type(x)!=dict or type(y)!=dict:
                    return str(y) + str(x)
                else:
                    return { key: value - x[key] for (key,value) in y.items() }
            values = []
            for i in range(commandstats_df_end.shape[0]): 
                begin_dict_list = [dic for dic in commandstats_df_begin.iloc[i]]
                end_dict_list = [dic for dic in commandstats_df_end.iloc[i]]
                result = []
                for j in range(len(end_dict_list)):
                    result.append(f(begin_dict_list[i], end_dict_list[i]))
                values.append(result)
            commandstats_df_diff = pd.DataFrame(values, columns=commandstats_df_end.columns, index=commandstats_df_end.index)
            slow_df = bh.get_slow_cluster()
            
            
        else:
            commandstats_df_diff = commandstats_df_end - commandstats_df_begin
            slow_df = bh.get_slow()
        commandstats_df_report = pd.concat([commandstats_df_begin, commandstats_df_end, commandstats_df_diff], axis=1, sort=False)
        #multiple_dfs([time_df, system_metrics_df, slow_df, commandstats_df_report, info_df_middle], tab_name, record_directory + f'/{tab_name}.xlsx', 3)
        multiple_dfs([time_df, system_metrics_df, commandstats_df_report, slow_df], tab_name, record_directory + f'/{tab_name}.xlsx', 3)


