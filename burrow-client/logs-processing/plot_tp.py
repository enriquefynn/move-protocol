#!/usr/bin/env python

import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np


def multi_plot_absolute(tput_path):
    times = []
    txs_delta = []
    first = True
    initial_time = 0
    txs = 0
    prev_time = 0

    partition_id = tput_path.split('-')[-1]
    moved_path = '/'.join(tput_path.split('/')
                          [:-1]) + '/movedTo-moved2-partition-' + partition_id

    sum_moved = []
    moved_total = 0
    try:
        with open(moved_path, 'r') as mf:
            for line in mf:
                movedTo, moved2, time_moved = map(
                    lambda i: int(i), line.split())
                moved_total = movedTo + moved2
                sum_moved.append((moved_total, time_moved))
    except:
        pass

    all_moves_sum = sum(list(map(lambda i: i[0], sum_moved)))
    timestamps = []

    with open(tput_path, 'r') as f:
        i = 0
        for line in f:
            line = line.split()
            txs = int(line[0])
            timestamp = int(line[1])
            if first:
                prev_txs = txs - moved_total
                prev_time = timestamp
                initial_time = timestamp
                first = False
                continue

            timestamps.append(timestamp/1e9)
            if len(sum_moved) > 0:
                moves, time_moved = sum_moved[i]
                txs -= moves
            if txs - prev_txs < 0:
                print('Warning', txs - prev_txs, moves, time_moved)
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
    for i in range(len(txs_delta)):
        txs_delta[i] = txs_delta[i]/times[i]
    ax.plot(times_from_zero, txs_delta, '.')
    # ax.plot([i for i in range(len(txs_delta))], txs_delta, '.')

    print("Average: {}".format((txs - all_moves_sum) /
                               float((prev_time - initial_time)/1e9)), np.average(times))
    # ax.plot(prev_time - initial_time, txs/float((prev_time - initial_time)/1e9), '.')
    ax.set(xlabel='time (s)', ylabel='tx/s')
    ylim1, ylim2 = plt.ylim()
    plt.ylim((0, ylim2))
    # plt.show()
    return fig, ax


def get_stopped_time_partition(stop_path):
    try:
        with open(stop_path, 'r') as f:
            return int(f.readline()[:-1])
    except:
        return None


def plot_aggregated(tput_paths, delta_time=5):
    raw_tputs = []
    stopped_at = []
    for path in tput_paths:
        tput = []
        partition_id = path.split('-')[-1]
        move_path = '/'.join(path.split('/')
                             [:-1]) + '/movedTo-moved2-partition-' + partition_id
        stop_path = '/'.join(path.split('/')[:-1]) + \
            '/stopped-tx-stream-partition-' + partition_id
        stopped_at.append(get_stopped_time_partition(stop_path))
        with open(path, 'r') as f, open(move_path, 'r') as mp:
            # f.readline()
            for line in f:
                movedTo = 0
                moved2 = 0
                timestamp = 0
                try:
                    movedTo, moved2, timestamp = map(
                        lambda i: int(i), mp.readline()[:-1].split())
                except:
                    pass
                (total_txs, timestamp, validator) = line.split()
                total_txs = int(total_txs)
                timestamp = int(timestamp)
                total_txs = total_txs - movedTo - moved2
                assert(total_txs > 0)
                tput.append((timestamp, total_txs))

        raw_tputs.append(tput)

    # remove non-overlapping ends
    max_min = 0
    min_max = float('inf')
    for tput in raw_tputs:
        max_min = max(max_min, tput[0][0])
        min_max = min(min_max, tput[-1][0])
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
    first = True
    all_txs_sum = 0
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

            # previous_tput[tput_i] = tput[prev_idx][1] + tput[idx][1] - previous_tput[tput_i]
            # print(tput_i, previous_tput[tput_i])
            time_passed = (tput[idx][0] - tput[prev_idx][0])/1e9
            if time_passed == 0:
                running = False
                break
            all_txs_sum += tput[idx][1] - tput[prev_idx][1]
            if first == True:
                first_time = tput[idx][0]
                first = False
            last_time = tput[idx][0]
            aggregated_data.append(
                ((tput[idx][0] + tput[prev_idx][0])/2., (tput[idx][1] - tput[prev_idx][1])/time_passed))
        if running == False:
            break
        avg_time = np.average(list(map(lambda i: i[0], aggregated_data)))
        sum_txs = sum(list(map(lambda i: i[1], aggregated_data)))
        aggregated_tputs.append((avg_time, sum_txs))
        start_time += delta

    print(last_time - first_time, all_txs_sum)

    # plot
    fig, ax = plt.subplots()
    avg_times = list(map(lambda i: (i[0] - max_min)/1e9, aggregated_tputs))

    ax.plot(avg_times, list(map(lambda i: i[1], aggregated_tputs)))
    ax.set(xlabel='time (s)', ylabel='tx/s')
    ylim1, ylim2 = plt.ylim()
    plt.ylim((0, ylim2))
    # plt.show()

    # Plot stopped lines
    labels = {'label': 'Limit reached'}
    for stop in stopped_at:
        if stop != None:
            plt.axvline(x=(stop - max_min)/1e9, color='r',
                        linestyle='--', **labels)
        labels = {}

    return fig, ax


if __name__ == '__main__':
    fig, ax = plot_aggregated(sys.argv[1:], delta_time=60)
    ax.legend()
    # plt.show()
    # fig, ax = multi_plot_absolute(sys.argv[1])
    # plt.show()
    path = sys.argv[1].split('/')
    filename = path[-1].split('.')[0]
    # path = '/'.join(path[:-1]) + '/' + filename + '.pdf'
    path = '/'.join(path[:-1]) + '/' + filename + '-aggregated.pdf'
    # path = '/'.join(path[:-1]) + '/' + filename + '-bloc-txs.pdf'
    print('saved at {}'.format(path))
    fig.savefig(path)
