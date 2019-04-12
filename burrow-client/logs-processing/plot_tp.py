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
        lines = list(map(lambda i : list(map(lambda l : int(l), (i[:-1]).split())), lines))
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
    ax.plot([i*delta_time for i in range(0, len(acc_data))], acc_data)
    ax.set(xlabel='time (s)', ylabel='tx/s', title='tput {} partitions'.format(len(tput_paths)))
    plt.show()
    return fig, ax

if __name__ == '__main__':
    fig, ax = multi_plot(sys.argv[1:], delta_time=5)
    fig.savefig('tput.pdf')
