#!/Users/fynn/.virtualenvs/ethereum/bin/python

import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

def to_relative_data(data, start_time):
    new_data = []
    for d in data:
        new_el = []
        idx = 0
        for el in d[1:]:
            new_el.append((el[0] - d[idx][0], el[1]-start_time))
            idx += 1
        new_data.append(new_el)
    return new_data

def aggregate(data, start_time, end_time, delta_time = 1):
    i = 0
    start = start_time
    delta = delta_time*1e9
    run = True
    acc_data = []
    
    idxs = [0 for i in range(len(data))]
    while run:
        acc = 0
        t = 0
        for d in range(len(data)):
            while idxs[d] < len(data[d]):
                if data[d][idxs[d]][1] > delta:
                    break
                acc += data[d][idxs[d]][0]
                idxs[d] += 1
            if idxs[d] == len(data[d]):
                run = False
        delta+=delta_time*1e9
        acc_data.append(acc)
        acc = 0
    return acc_data

def multi_plot_absolute(lat_path):
    times = []
    txs_delta = []
    first = True
    initial_time = 0
    txs = 0
    prev_time = 0
    validators = []
    with open(lat_path, 'r') as f:
        for line in f:
            line = line.split()
            txs = int(line[0])
            timestamp = int(line[1])
            validators.append(line[2])
            if first:
                prev_txs = txs 
                prev_time = timestamp 
                initial_time = timestamp
                first = False
                continue
            txs_delta.append(txs - prev_txs)
            times.append((timestamp - prev_time)/1e9)
            prev_time = timestamp
            prev_txs = txs

    fig, ax = plt.subplots()
    # ax.plot(times, txs_delta, '.')
    for i in range(len(txs_delta)):
        txs_delta[i] = txs_delta[i]/times[i]
    ax.plot([i for i in range(len(txs_delta))], times, '.')
    print("Average: {}".format(np.average(times)))
    for i in range(len(times)):
        if times[i] > 20:
            print(validators[i])
    # ax.plot(prev_time - initial_time, txs/float((prev_time - initial_time)/1e9), '.')
    ax.set(xlabel='time (s)', ylabel='latency')
    plt.show()
    return fig, ax

if __name__ == '__main__':
    # fig, ax = multi_plot(sys.argv[1:], delta_time=400)
    fig, ax = multi_plot_absolute(sys.argv[1])
    path = sys.argv[1].split('/')
    filename = path[-1].split('.')[0]
    path = '/'.join(path[:-1]) + '/' + filename + '_lat.pdf'
    fig.savefig(path)
