#!/Users/fynn/.pyenv/eth/bin/python

import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

def get_moves_per_partition(latencies_path):
    moves_per_partition = {'1': {}}
    with open(latencies_path, 'r') as f:
        for line in f:
            line = line.split()
            if line[1] == 'moveTo' or line[1] == 'move2':
                partition = line[2]
                height = int(line[3])
                if partition not in moves_per_partition:
                    moves_per_partition[partition] = {}
                if height not in moves_per_partition[partition]:
                    moves_per_partition[partition][height] = 0
                moves_per_partition[partition][height]+=1

    return moves_per_partition


def multi_plot_absolute(tput_path):
    times = []
    txs_delta = []
    first = True
    txs = 0
    prev_time = 0

    partition_id = tput_path.split('-')[-1].split('.')[0]

    timestamps = []

    begin_path = '/'.join(tput_path.split('/')[:-1]) + '/begin-experiment.txt'
    with open(begin_path, 'r') as be:
        experiment_begin = int(be.readline())

    moves_per_partition = get_moves_per_partition('/'.join(tput_path.split('/')[:-1]) + '/latencies.txt')
    moves_in_partition = moves_per_partition[partition_id]

    with open(tput_path, 'r') as f:
        i = 0
        for line in f:
            line = line.split()
            txs = int(line[0])
            timestamp = int(line[1])
            height = int(line[2])
            if timestamp < experiment_begin:
                continue
            if first:
                initial_time = timestamp
                prev_txs = txs
                prev_time = timestamp 
                first = False
                continue
            timestamps.append(timestamp/1e9)
            moves = 0
            for ts in moves_in_partition:
                if ts >= prev_time and ts <= timestamp:
                    moves+=1
            if height in moves_per_partition[partition_id]:
                txs -= moves_per_partition[partition_id][height]
            if txs - prev_txs < 0:
                print('Warning', txs - prev_txs, moves)
            txs_delta.append(txs - prev_txs)
            times.append((timestamp - prev_time)/1e9)
            prev_time = timestamp
            prev_txs = txs
            i += 1

    fig, ax = plt.subplots()
    times_from_zero = [0]
    for t in times[1:]:
        times_from_zero.append(times_from_zero[-1] + t)
    # ax.plot(times, txs_delta, '.')
    # print('Avg latency:', np.average(times))
    # print('Avg txs/s:', sum(txs_delta)/((timestamp - initial_time)/1e9))
    for i in range(len(txs_delta)):
        txs_delta[i] = txs_delta[i]/times[i]
    ax.plot(times_from_zero, txs_delta, '.')
    # ax.plot([i for i in range(len(txs_delta))], txs_delta, '.')

    # ax.plot(prev_time - initial_time, txs/float((prev_time - initial_time)/1e9), '.')
    ax.set(xlabel='time (s)', ylabel='tx/s')
    ylim1, ylim2 = plt.ylim()
    plt.ylim((0, ylim2))
    plt.title('Sending to multiple clients (1000 clients)')
    plt.show()
    return fig, ax


def get_stopped_time_partition(stop_path):
    try:
        with open(stop_path, 'r') as f:
            return int(f.readline()[:-1])
    except:
        return None

def plot_aggregated(tput_paths, delta_time=5):
    raw_tputs = []
    for path in tput_paths:
        tput = []
        partition_id = path.split('-')[-1].split('.')[0]
        begin_path = '/'.join(path.split('/')[:-1]) + '/begin-experiment.txt'
        with open(begin_path, 'r') as be:
            experiment_begin = int(be.readline())
        moves_per_partition = get_moves_per_partition('/'.join(path.split('/')[:-1]) + '/latencies.txt')

        with open(path, 'r') as f:
            # f.readline()
            for line in f:
                timestamp = 0
                (total_txs, timestamp, height, validator) = line.split()
                total_txs = int(total_txs)
                timestamp = int(timestamp)
                height = int(height)
                if timestamp < experiment_begin:
                    continue
                
                if height in moves_per_partition[partition_id]:
                    total_txs -= moves_per_partition[partition_id][height]
                assert(total_txs >= 0)
                tput.append((timestamp, total_txs))

        raw_tputs.append(tput)
    
    # remove non-overlapping ends
    max_min = 0
    min_max = float('inf')
    for tput in raw_tputs:
        max_min = max(max_min, tput[0][0])
        min_max  = min(min_max, tput[-1][0])
    for tput in raw_tputs:
        while tput[0][0] < max_min:
            tput.remove(tput[0])
        while tput[-1][0] > min_max:
            tput.pop()
    
    aggregated_tputs = []

    delta = delta_time*1e9
    start_time = max_min

    data_points_i = [0 for i in range(len(raw_tputs))]
    running = True
    # Aggregate
    while True:
        aggregated_data = []
        for tput_i, tput in enumerate(raw_tputs):
            prev_idx = data_points_i[tput_i]
            while True:
                idx = data_points_i[tput_i]
                if idx >= len(tput):
                    running = False
                    break
                data_points_i[tput_i] += 1
                if tput[idx][0] > start_time + delta:
                    break
            
            if running == False:
                break

            time_passed = (tput[idx][0] - tput[prev_idx][0])/1e9
            if time_passed == 0:
                running = False
                break
            aggregated_data.append(((tput[idx][0] + tput[prev_idx][0])/2., (tput[idx][1] - tput[prev_idx][1])/time_passed ))
        if running == False:
            break
        avg_time = np.average(list(map(lambda i : i[0], aggregated_data)))
        sum_txs = sum(list(map(lambda i : i[1], aggregated_data)))

        aggregated_tputs.append((avg_time, sum_txs))
        start_time += delta


    # plot
    fig, ax = plt.subplots()
    avg_times = list(map(lambda i : (i[0] - max_min)/1e9, aggregated_tputs))

    ax.plot(avg_times, list(map(lambda i : i[1], aggregated_tputs)))
    ax.set(xlabel='time (s)', ylabel='tx/s', title='tput {} partitions'.format(len(tput_paths)))
    ylim1, ylim2 = plt.ylim()
    plt.ylim((0, ylim2))
    # plt.show()
    
    # for i in range(len(avg_times)):
    #     print('{} {}'.format(avg_times[i],aggregated_tputs[i][1]))

    total_tput = 0
    for p in raw_tputs:
        total_tput += p[-1][1]
    print(min_max - max_min, total_tput)


    return fig, ax
                
if __name__ == '__main__':
    partitions = int(sys.argv[1])
    tput_paths = ['{}/tput-partition-{}.txt'.format(sys.argv[2],i+1) for i in range(partitions)]
    fig, ax = plot_aggregated(tput_paths, delta_time=60)
    path = sys.argv[2] + '/' + 'tput-aggregated.pdf'
    fig.savefig(path)

    for tp in tput_paths:
        fig, ax = multi_plot_absolute(tp) 
        p = tp[:-3] + 'pdf'
        fig.savefig(p)