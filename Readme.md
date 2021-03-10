# Sharding ethereum experiments

There is one genesis file per shard, each one managing their own blockchain + beacon chain `beacon_genesis.json`

There is one validator per shard. 

Clients that want so submit cross-shard transactions between `s1` and `s2` should listen to `s1` and `s2` (at least the
headers)


Executing a move:
Client c, move from shard s1 to s2.
c craft transaction TxMove1 with move opcode, send to s1

s1 receives TxMove1, executes, does not refund client right away.
s1 spits block B1 (signed), txMove1 \in B1

c crafts transaction TxMove2, together with B1, send to s2
s2 verifies that this is indeed signed by s1.
s2 reconstructs state of contract in s2
s2 spits block B2, txMove2 \in B2

c crafts transaction to delete state in s1,
s1 keeps the headers (important to avoid replay attacks)

Ethereum client:
https://github.com/enriquefynn/go-ethereum/tree/sharding
