#!/usr/bin/env python3
import sys

current_term = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t', 1)
    term = parts[0]
    count = int(parts[1])

    if term == current_term:
        current_count += count
    else:
        if current_term is not None:
            print('%s\t%d' % (current_term, current_count))
        current_term = term
        current_count = count

if current_term is not None:
    print('%s\t%d' % (current_term, current_count))
