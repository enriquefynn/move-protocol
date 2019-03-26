#!/Users/fynn/.pyenv/eth/bin/python

import sys
import matplotlib
import matplotlib.pyplot as plt

def plot(tput_path):
    txs, time_diffs = ([], [])
    with open(tput_path, 'r') as f:
        begin_at = 0
        last_time = 0
        last_total_tx = 0
        first = True
        tx = 0
        for line in f:
            (total_tx, abs_time) = map(lambda i : int(i), line.split())
            if first:
                begin_at = abs_time
                last_total_tx = total_tx
                last_time = abs_time
                first = False
                continue
            time_diff = abs_time - last_time

            tx += (total_tx - last_total_tx) / (time_diff/1e9)
            if time_diff/1e9 >= 1:
                txs.append(tx)
                time_diffs.append((abs_time - begin_at)/1e9)

                last_time = abs_time
                last_total_tx = total_tx
                tx = 0

    fig, ax = plt.subplots()
    ax.plot(time_diffs, txs)
    ax.set(xlabel='time (s)', ylabel='tx/s', title='tput')
    plt.show()
    return fig, ax
    

if __name__ == '__main__':
    plot_file = sys.argv[1]
    fig, ax = plot(plot_file)
    fig.savefig('tput.pdf')
