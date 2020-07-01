import boto3
from datetime import datetime, timezone

# Create CloudWatch client

def get_metric_average(cache_cluster_id, metric, start_time, end_time, region='ap-northeast-1'):

    cloudwatch = boto3.client('cloudwatch', region_name=region)
    response = cloudwatch.get_metric_statistics(
    Namespace='AWS/ElastiCache',
    MetricName=metric,
    Dimensions=[
        {
            'Name': 'CacheClusterId',
            'Value': cache_cluster_id,
        },
    ],
    StartTime=start_time,
    EndTime=end_time,
    #Period=int((end_time - start_time).total_seconds()),
    Period=60,
    Statistics=[
        'Average', 'Minimum', 'Maximum',
    ]
    )
    return response

