import random
import string
import sys

a = ['  uint a{} = 0x{};'.format(i, "".join([random.choice(string.hexdigits) for x in range(64)])) for i in range(int(sys.argv[1]))]

for i in a:
  print(i)
