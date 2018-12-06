#!/usr/bin/env sh
for i in `seq 1 4`
do
  echo cleaning validator $i
  tendermint unsafe_reset_all --home ./validator$i > /dev/null
done
rm */*/write*
