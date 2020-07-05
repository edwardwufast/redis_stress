from datetime import datetime
from statistics import mean

import pandas as pd

from awsapi import get_metric_average



def test_get_metrics_df():
    start_time = '2020-06-30 06:00:00'
    end_time = '2020-06-30 07:30:00'
    server = 'redisconnect.ryi7vr.0001.apne1.cache.amazonaws.com' 

    start_time_datetime = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_time_datetime = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    wanted_cloudwatch_metrics = ['EngineCPUUtilization', 'NetworkBytesIn', 'NetworkBytesOut', 'NewConnections', 'CurrConnections']
    cloudwatch_metrics = [get_metric_average(server.split('.')[0], metric, start_time_datetime, end_time_datetime) for metric in wanted_cloudwatch_metrics]
    cloudwatch_metrics[0]['Label']
    cloudwatch_metrics_avg = {metric['Label']:mean([data['Average'] for data in metric['Datapoints']]) for metric in cloudwatch_metrics}
    system_metrics_df1 = pd.DataFrame(data=cloudwatch_metrics_avg, index={0}).T
    print(system_metrics_df1)
