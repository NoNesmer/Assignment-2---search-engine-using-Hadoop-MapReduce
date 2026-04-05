#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) >= 1:
        # each line in the index is one (term, doc) pair
        # emit term with count 1 so reducer can sum df
        print('%s\t1' % parts[0])
