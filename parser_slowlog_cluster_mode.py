#!/usr/bin/env python3
import dns.resolver
import re
import sys

from datetime import datetime

ReplicationGroupId = sys.argv[1]
node_endpoint_tail = sys.argv[2]
number_shard = str(sys.argv[3])
number_replica = str(sys.argv[4])

def unix_to_utc(match):
    unix_time = int(match.groups()[0])
    return '2) ' + datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S UTC')



MemberClusters = [ f'{ReplicationGroupId}-000{i}-00{j}' + '.' + node_endpoint_tail for i in range(1, int(number_shard)+1) for j in range(1, int(number_replica)+1)]

def get_IP(domain_name):
    return [ ip.to_text() for ip in dns.resolver.resolve(domain_name, 'A')][0]

mapping = { get_IP(member): member for member in MemberClusters }


if __name__ == "__main__":

    with open(sys.argv[5], errors='ignore') as f:
        read_data = f.read()
    pattern = re.compile(r"(\d{10,})")
    result_text = pattern.sub(unix_to_utc, read_data)
    for IP in mapping:
        result_text = re.sub(IP, mapping[IP] + '  ' + IP, result_text)
    print(result_text)
    
