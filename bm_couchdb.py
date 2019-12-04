import couchdb
import time
import pywren_ibm_cloud as pywren
import argparse
import pika
import os
import yaml
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

DST_DIR_PLOTS = 'plots'
DST_DIR_CSV = 'csv'
DST_FILE_NAME = 'couchdb_{}{}_{}sec_{}bytes' # leave as is

# Edit this
COUCHDB_ENDPOINT = ''

EXCHANGE_NAME = 'benchmark_fanout' # temporary, will get deleted (!)
DB_NAME = 'benchmark_db'   # temporary, will get deleted (!)

class TriggerCallback:
    def __call__(self, ch: pika.adapters.blocking_connection.BlockingChannel, method, properties, body):
            ch.stop_consuming()

def writer(id, rabbitmq: pika.BlockingConnection, burst_time, size, trigger_queue_name):
    timestamps = []
    doc = {'body': '0' * size}
    key = str(id)   # unique for each function

    # Connect to couchdb
    server = couchdb.Server(COUCHDB_ENDPOINT)
    db = server[DB_NAME]

    time.sleep(1)

    # Declare the queue where we will be sent the firing message (through the exchange)
    ch = rabbitmq.channel()
    res = ch.queue_declare(queue='', exclusive=True)
    own_queue_name = res.method.queue
    ch.queue_bind(exchange=EXCHANGE_NAME, queue=own_queue_name)
    ch.basic_consume(consumer_callback=TriggerCallback(), queue=own_queue_name, no_ack=True)

    # Tell the client we're ready and wait for its message
    ch.basic_publish(exchange='', routing_key=trigger_queue_name, body='-')
    ch.start_consuming()

    # Write/send documents and save the timestamps
    start_time = ts = time.time()
    while (ts - start_time < burst_time):
        ts = time.time()
        db[key] = doc
        timestamps.append(ts)

    rabbitmq.close()
    return timestamps

def reader(id, rabbitmq: pika.BlockingConnection, burst_time, size, trigger_queue_name):
    timestamps = []
    key = str(id)   # unique for each function

    # Connect to couchdb
    server = couchdb.Server(COUCHDB_ENDPOINT)
    db = server[DB_NAME]
    db[key] = {'body': '0' * size}

    time.sleep(2)

    # Declare the queue where we will be sent the firing message (through the exchange)
    ch = rabbitmq.channel()
    res = ch.queue_declare(queue='', exclusive=True)
    own_queue_name = res.method.queue
    ch.queue_bind(exchange=EXCHANGE_NAME, queue=own_queue_name)
    ch.basic_consume(consumer_callback=TriggerCallback(), queue=own_queue_name, no_ack=True)

    # Tell the client we're ready and wait for its message
    ch.basic_publish(exchange='', routing_key=trigger_queue_name, body='-')
    ch.start_consuming()

    # Read/get documents and save the timestamps
    start_time = ts = time.time()
    while (ts - start_time < burst_time):
        ts = time.time()
        if db.get(key) is None:
            raise Exception('Reader couldn\'t read') # never happens
        timestamps.append(ts)

    rabbitmq.close()
    return timestamps

class CounterCallback:
    def __init__(self, counter):
        self.counter = counter

    def __call__(self, ch: pika.adapters.blocking_connection.BlockingChannel, method, properties, body):
        self.counter -= 1
        print(self.counter)

        if self.counter == 0:
            ch.stop_consuming()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', type=int, default=1,
                        help='Number of writers/readers')
    parser.add_argument('-t', type=int, default=5,
                        help='Burst time')
    parser.add_argument('-s', type=int, default=10,
                        help='Message size (bytes)')
    parser.add_argument('--write', action='store_true', default=False)
    parser.add_argument('--read', action='store_true', default=False)
    args = parser.parse_args()

    num_invokes = args.n
    burst_time = args.t
    msg_size = args.s
    test_write = args.write
    test_read = args.read
    if test_write == test_read:
        test_write = True
        test_read = False
    
    server = couchdb.Server(COUCHDB_ENDPOINT)
    try:
        db = server.create(DB_NAME)
    except:
        # if it already exists,
        # reset to aviod conflicts with ids
        server.delete(DB_NAME)
        db = server.create(DB_NAME)

    with open(os.path.expanduser('~/.pywren_config'), 'r') as f:
        config = yaml.safe_load(f)
    pika_params = pika.URLParameters(config['rabbitmq']['amqp_url'])
    connection = pika.BlockingConnection(pika_params)
    ch = connection.channel()

    # Declare the exchange to trigger the benchmark
    ch.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout')
    
    # Declare the queue to wait for all ready messages
    res = ch.queue_declare(queue='', exclusive=True)
    queue_name = res.method.queue
    ch.basic_consume(consumer_callback=CounterCallback(num_invokes), queue=queue_name, no_ack=True)

    # Invoke functions, wait for all of them to be ready (cold starts)
    func = writer if test_write else reader
    pw = pywren.ibm_cf_executor()
    pw.map(func, [[burst_time, msg_size, queue_name]] * num_invokes)
    ch.start_consuming()
    time.sleep(0.5)

    # Start the benchmark by sending the trigger message
    ch.basic_publish(exchange=EXCHANGE_NAME, routing_key='', body='-')
    zero_time = time.time()
    print('Fire!')

    try:
        results = pw.get_result()
    finally:
        del server[DB_NAME]
        ch.exchange_delete(exchange=EXCHANGE_NAME)
        connection.close()

    # Process results
    timestamps = results
    time_range = burst_time * 3
    max_sec = 0
    total = 0
    intervals = [[0] * time_range for _ in range(num_invokes)]
    
    for i, res in enumerate(timestamps):
        for ts in res:
            sec = int(ts - zero_time)
            intervals[i][sec] += 1
            max_sec = sec if sec > max_sec else max_sec
        suma = sum(intervals[i])
        total += suma
        print(intervals[i], 'sum =', suma)
    print('Total {}: {}'.format('writes' if test_write else 'reads', total))

    num_itvs = max_sec + 1

    sum_intervals = [0] * num_itvs
    avg_intervals = [0] * num_itvs
    for i in range(num_itvs):
        acum = 0
        for itv in intervals:
            acum += itv[i]
        sum_intervals[i] = acum
        avg_intervals[i] = acum / num_invokes


    def write_csv(intervals, file_name):
        if not os.path.isdir(DST_DIR_CSV):
            os.mkdir(DST_DIR_CSV)

        file_path = os.path.join(DST_DIR_CSV, file_name)
        print('Writing csv file... ({})'.format(file_path))
        with open(file_path, 'w') as csv_file:
            csv_file.write('interval_sec,num_actions\n')
            for sec, n in enumerate(intervals):
                csv_file.write('{},{}\n'.format(sec, n))

    # Save results to csv file
    file_name = DST_FILE_NAME.format(num_invokes, 
                                    'writers' if test_write else 'readers',
                                     burst_time,
                                     msg_size)
    write_csv(avg_intervals, file_name+'_average.csv')
    write_csv(sum_intervals, file_name+'_sum.csv')

    def plot_lines(intervals, label, file_name):
        fig, ax = plt.subplots()
        title = 'Couchdb {} benchmark \n {}{} {}sec {}bytes/msg'.format(
                    'write' if test_write else 'read',
                    num_invokes, 
                    'writers' if test_write else 'readers',
                    burst_time,
                    msg_size)
        ax.set(xlabel='time (s)', ylabel='number of writes' if test_write else 'number of reads',
                title=title)

        x = np.arange(0, num_itvs, 1.0)
        plt.xticks(x)

        # Plot lines
        ax.plot(x, intervals, label=label)
        ax.grid()
        ax.legend()

        if not os.path.isdir(DST_DIR_PLOTS):
            os.mkdir(DST_DIR_PLOTS)

        file_path = os.path.join(DST_DIR_PLOTS, file_name)
        print('Saving plotted graph... ({})'.format(file_path))
        fig.savefig(file_path)
        #plt.show()
    
    plot_lines(avg_intervals, 'average per worker', file_name+'_average')
    plot_lines(sum_intervals, 'total amount', file_name+'_sum')

