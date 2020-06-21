#!/usr/bin/env python3

import re
import sys

from datetime import datetime

def unix_to_utc(match):
    unix_time = int(match.groups()[0])
    return '2) ' + datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S UTC')

if __name__ == "__main__":

    with open(sys.argv[1]) as f:
        read_data = f.read()
    pattern = re.compile(r"2\) \(integer\) (\d+)")
    print(pattern.sub(unix_to_utc, read_data))
