#!/Users/fynn/.virtualenvs/ethereum/bin/python

import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

def plot_cdf(p):
    plt.style.use('grayscale')
    latencies = []
    with open(p, 'r') as f:
        for line in f:
            line = line.split()
            latencies.append((int(line[2]) - int(line[1]))/1e9)
    
    latencies = latencies[int(len(latencies)*0.1):int(len(latencies)*0.9)]

    data_set = sorted(set(latencies))
    num_bins = np.append(data_set, data_set[-1] + 1)
     # Use the histogram function to bin the data
    counts, bin_edges = np.histogram(latencies, bins=num_bins)  # , normed=True)
    counts = counts.astype(float) / len(latencies)
    # Now find the cdf
    cdf = np.cumsum(counts)
    cdf_plot = {'value': bin_edges[0:-1], 'percentage': cdf}
    fig, ax = plt.subplots()
    ax.set_xscale('log')

    ax.plot(bin_edges[0:-1], cdf)
    #ax.ylim((0, 1))

    return fig, ax
    


if __name__ == '__main__':
    # fig, ax = multi_plot(sys.argv[1:], delta_time=400)
    path = sys.argv[1]
    fig, ax = plot_cdf(path)
    path = sys.argv[1].split('/')
    filename = path[-1].split('.')[0]
    path = '/'.join(path[:-1]) + '/' + filename + '_cdf.pdf'
    fig.savefig(path)
