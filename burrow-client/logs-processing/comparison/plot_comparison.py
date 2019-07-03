#!/Users/fynn/.pyenv/eth/bin/python

import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

from cycler import cycler

# Create cycler object. Use any styling from above you please
bar_cycle = (cycler('hatch', ['///', '--', '...','\///', 'xxx', '\\\\']) * cycler('color', 'w')*cycler('zorder', [10]))
styles = bar_cycle()

folder_name = '0_percent'
def load_0p(folder):
    partitions=[1,2,4,8]
    tputs, times = ([], [])
    for p in partitions:
        with open(folder + '/{}p_250.txt'.format(p), 'r') as f:
            time, tput = map(lambda i : int(i), f.readline()[:-1].split()) 
            times.append(int(time))
            tputs.append(int(tput))
        
    return times, tputs

def plot_comparison(tput_paths, cross_shard, real_workload=False):
    fig, ax = plt.subplots()
    # ax.set_prop_cycle(monochrome)

    base_times, base_tput = load_0p(folder_name)

    base_tps = []
    for i, tp in enumerate(base_tput):
        base_tps.append(tp/(base_times[i]/1e9))
    print(base_tps)

    for idx, p in enumerate(tput_paths):
        with open(p, 'r') as f:
            time, tput = map(lambda i : int(i), f.readline()[:-1].split())
        if idx == 0 and real_workload == False:
            ax.bar(idx, tput/(time/1e9), color=(0.2, 0.4, 0.6, 0.6), edgecolor='black', label='{}% cross-shard'.format(cross_shard))
        else:
           ax.bar(idx, tput/(time/1e9), color=(0.2, 0.4, 0.6, 0.6), edgecolor='black') 

        if idx > 0 and real_workload == False:
            if idx == 1:
                ax.bar(idx, base_tps[idx] - tput/(time/1e9), color='lightblue', hatch='//', bottom=tput/(time/1e9), edgecolor='black', label='0% cross-shard')
            else:
                ax.bar(idx, base_tps[idx] - tput/(time/1e9), color='lightblue', hatch='//', bottom=tput/(time/1e9), edgecolor='black')

    if real_workload == False:
        ax.legend()
    ax.set_yticks([0,50,100,150])
    ax.set_xticks([0,1,2,3])
    ax.set_xticklabels([1,2,4,8])
    ax.set_xlabel('# shards')
    ax.set_ylabel('txs/s')
    # ax.set_title('{}%'.format(cross_shard))

    return fig, ax


                
if __name__ == '__main__':
    path = sys.argv[3].split('/')
    cross_shard = int(sys.argv[1])
    filename = path[-1].split('.')[0]
    fig, ax = plot_comparison(sys.argv[2:], cross_shard, real_workload=True)
    path = '/'.join(path[:-1]) + '/' + 'tput-comparison.pdf'
    fig.savefig(path)