#!/Users/fynn/.pyenv/eth/bin/python

import sys
import matplotlib
import matplotlib.pyplot as plt

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

def multi_plot_absolute(tput_path):
    times = []
    txs_delta = []
    first = True
    initial_time = 0
    txs = 0
    prev_time = 0
    with open(tput_path, 'r') as f:
        for line in f:
            line = line.split()
            txs = int(line[0])
            timestamp = int(line[1])
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
    ax.plot([i for i in range(len(txs_delta))], txs_delta, '.')
    print("Average: {}".format(txs/float((prev_time - initial_time)/1e9)))
    # ax.plot(prev_time - initial_time, txs/float((prev_time - initial_time)/1e9), '.')
    ax.set(xlabel='time (s)', ylabel='tx/s')
    plt.show()
    return fig, ax


def multi_plot(tput_paths, delta_time = 1):
    files = []
    for path in tput_paths:
        files.append(open(path, 'r'))

    max_time = 0
    min_time = float('inf')
    # Get max
    raw_data = []
    min_elements = float('inf')
    for f in files:
        lines = f.readlines()
        lines = list(map(lambda i : i[:-1].split(), lines))
        lines = list(map(lambda i : i[:2], lines))
        lines = list(map(lambda i : list(map(lambda j : int(j) , i)), lines))
        raw_data.append(lines)
        if len(lines) < min_elements:
            min_elements = len(lines)
        
    for d in raw_data:
        if d[0][1] > max_time:
            max_time = d[0][1]
        if d[-1][1] < min_time:
            min_time = d[-1][1]
        
    for d in raw_data:
        for i in range(len(d)):
            if d[i][1] >= max_time:
                break
        del d[:i]
        for i in range(len(d)-1, -1, -1):
            if d[i][1] <= min_time:
                break
        del d[i+1:]

    
    data = to_relative_data(raw_data, max_time) 
    acc_data = aggregate(data, max_time, min_time, delta_time=delta_time)
    acc_data = list(map(lambda i : float(i/delta_time), acc_data))

    fig, ax = plt.subplots()
    ax.plot([i*delta_time for i in range(0, len(acc_data))], acc_data, '.')
    ax.set(xlabel='time (s)', ylabel='tx/s', title='tput {} partitions'.format(len(tput_paths)))
    plt.show()
    return fig, ax

if __name__ == '__main__':
    # fig, ax = multi_plot(sys.argv[1:], delta_time=400)
    fig, ax = multi_plot_absolute(sys.argv[1])
    fig.savefig('tput.pdf')
