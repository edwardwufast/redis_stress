import time
import argparse
import threading

from benchmark import benchmark, send_psetex, send_evalsha, create_dir

parser = argparse.ArgumentParser(description='Redis benchmark')

parser.add_argument('-s', metavar='redis_server_IP', required=True)
parser.add_argument('-c', metavar='connection', type=int, required=True)
parser.add_argument('-d', metavar='record_directory', required=True)
parser.add_argument('-t', metavar='test_time', type=int, required=True)

args = parser.parse_args()

if __name__ == '__main__':
    server = args.s
    connection = args.c
    record_directory = args.d
    test_time = args.t

    create_dir(record_directory)
    bh = benchmark(record_directory, test_time, send_psetex, server)
    bh.init_client()
    bh.save_info('begin')
    threads = list()
    for index in range(connection):
        x = threading.Thread(target=bh.run)
        threads.append(x)
        x.start()
    time.sleep(10)
    bh.save_info('middle')
    for index, thread in enumerate(threads):
        thread.join()
    bh.save_info('end')
    bh.save_slow('slow')



