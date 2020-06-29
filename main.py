import time
import argparse
import threading

from benchmark import benchmark, send_psetex, send_evalsha, create_dir, send_set_randomkey

benchmark_dict = {'send_psetex': send_psetex, 'send_evalsha': send_evalsha, 'send_set_randomkey': send_set_randomkey}

parser = argparse.ArgumentParser(description='Redis benchmark')

parser.add_argument('-s', metavar='redis_server_IP', required=True)
parser.add_argument('-c', metavar='connection', type=int, required=True)
parser.add_argument('-d', metavar='record_directory', required=True)
parser.add_argument('-t', metavar='test_time', type=int, required=True)
parser.add_argument('-b', metavar='benchmark', choices=[key for key in benchmark_dict], required=True)
parser.add_argument('-C', help='cluster mode', default=False, action='store_true', required=False)
parser.add_argument('-l', metavar='lua script file path for send_evalsha', required=False)

args = parser.parse_args()


if __name__ == '__main__':
    server = args.s
    connection = args.c
    record_directory = args.d
    test_time = args.t
    benchmark_test = benchmark_dict[args.b]
    cluster_mode_enable = args.C
    script_path = args.l
    if benchmark_test == send_evalsha:
        if not script_path:
            print("Please provide lua script in -l option")
            raise
        else:
            with open(script_path) as script_fd:
                script = script_fd.read()
            send_evalsha.load_script(script)

    create_dir(record_directory)
    bh = benchmark(record_directory, test_time, benchmark_test, server)
    if cluster_mode_enable:
        bh.init_cluster_client()
    else:
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



