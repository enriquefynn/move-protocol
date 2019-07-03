#!/Users/fynn/.pyenv/eth/bin/python

import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

def plot_micro(micro_file):
    width = 0.35
    fig, axs = plt.subplots(2, 1, sharex=True)
    # fig2, ax2 = plt.subplots()
    x = []
    y = []
    with open(micro_file[0], 'r') as mf:
        for line in mf:
            (xx, yy) = map(lambda i : int(i), line.split())
            x.append(xx)
            y.append(yy/1e3)

    # ax.plot(x, y, linestyle='--', marker='*', label='proofs size')
    axs[0].bar([i+width for i in range(len(y))], y, width, label='proofs size', edgecolor='black', color=(0.2, 0.4, 0.6, 0.6))
    x = []
    y = []
    with open(micro_file[1], 'r') as mf:
        for line in mf:
            (xx, yy) = map(lambda i : int(i), line.split())
            x.append(xx)
            y.append(yy/1e3)

    axs[1].bar([i+width for i in range(len(y))], y, width, label='Header size', edgecolor='black', color='lightblue', hatch='//')
    # ax.plot(x, y, linestyle='--', marker='x', label='header size')

    # ax.set_yscale('log')
    # ax.set_xscale('log')
    axs[0].set(xlabel='# 32 bytes storage', ylabel='Storage proof size (kb)')
    axs[1].set(xlabel='# validators', ylabel='Header size (kb)')
    # axs[1].set(xlabel='# 32 bytes storage/validators', ylabel='Storage proof/Header size (kb)')
    # plt.show()

    # ax.set_yticks([0,50,100,150,200,250,300])
    axs[1].set_xticks([i+width for i in range(10)])
    axs[1].set_xticklabels([2**i for i in range(10)])

    axs[0].set_xticks([i+width for i in range(10)])
    axs[0].set_xticklabels([2**i for i in range(10)])

    # axs[1].legend()
    return fig, axs[0]

if __name__ == '__main__':
    fig, ax = plot_micro(sys.argv[1:])
    path = sys.argv[1].split('/')
    filename = path[-1].split('.')[0]
    # path = '/'.join(path[:-1]) + '/' + filename + '.pdf'
    path = '/'.join(path[:-1]) + '/' + 'storage_header_size' + '.pdf'
    fig.savefig(path)