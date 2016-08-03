#!python/bin/python
# rand_subset_r.py

import sys

k = int(sys.argv[1])
c = 0

for line in sys.stdin:
    (r, x) = line.strip().split('\t', 1)
    print x
    c += 1
    if c == k:
        break