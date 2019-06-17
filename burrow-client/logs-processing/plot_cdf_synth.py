#!/Users/fynn/.virtualenvs/ethereum/bin/python

import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

def get_clients_latencies(latencies_path):
  clients = {}
  begin_path = '/'.join(latencies_path.split('/')[:-1]) + '/begin-experiment.txt'
  with open(begin_path, 'r') as be:
    begin_experiment = int(be.readline())
        
  with open(latencies_path, 'r') as f:
    for line in f:
      line = line.split()
      client_id = int(line[0])
      method = line[1]
      if method == 'moveTo' or method == 'move2':
        continue
      at = int(line[2])
      if at < begin_experiment:
        continue
      if method == 'gaveUp':
        clients[client_id][-1]['failed'] = True 
        continue
      lat = int(line[3])
      failed = line[-1] == 'true'
      if method == 'transfer':
        cross_shard = line[-2] == 'true'

      if client_id not in clients:
        clients[client_id] = [] 
      if len(clients[client_id]) >= 1 and clients[client_id][-1]['failed'] == True:
        clients[client_id][-1]['latency'] += lat
        clients[client_id][-1]['failed'] = failed
      else:
        clients[client_id].append({'method': method, 'latency': lat, 'failed': failed, 'cross_shard': cross_shard})
  return clients

def plot_cdf(clients):
    plt.style.use('grayscale')
    latencies = []
    for c in clients:
      latencies += map(lambda i : i['latency']/1e9, clients[c])
    print(len(latencies))

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
    # ax.set_xscale('log')

    ax.plot(bin_edges[0:-1], cdf)
    #ax.ylim((0, 1))

    return fig, ax
    


if __name__ == '__main__':
    # fig, ax = multi_plot(sys.argv[1:], delta_time=400)
    path = sys.argv[1]
    clients = get_clients_latencies(path)

    fig, ax = plot_cdf(clients)
    # plt.show()

    # fig, ax = plot_cdf(path)
    path = sys.argv[1].split('/')
    filename = path[-1].split('.')[0]
    path = '/'.join(path[:-1]) + '/' + filename + '_cdf.pdf'
    fig.savefig(path)
