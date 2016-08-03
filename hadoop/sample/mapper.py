#!python/bin/python

import sys, random
from heapq import heappush, heapreplace

k = int(sys.argv[1])
H = []

for x in sys.stdin:
    x  = x.strip()
    r = random.random()
    if len(H) < k:
        heappush(H, (r, x))
    elif r > H[0][0]:
        heapreplace(H, (r, x))

for (r, x) in H:
    # by negating the id, the reducer receives the elements from highest to lowest
    print '%f\t%s' % (-r, x)

