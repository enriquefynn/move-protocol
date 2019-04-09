#!/Users/fynn/.pyenv/eth/bin/python

import sys

if __name__ == '__main__':
    accounts = set() 
    with open(sys.argv[1], 'r') as f:
        for line in f:
            line = line.split()
            if line[0] == 'Transfer':
                accounts.add(line[2])
                accounts.add(line[4])
    print(len(accounts))