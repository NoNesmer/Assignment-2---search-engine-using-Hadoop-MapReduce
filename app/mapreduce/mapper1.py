#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t', 2)
    if len(parts) < 3:
        continue

    doc_id = parts[0]
    doc_title = parts[1]
    doc_text = parts[2]

    # tokenize: lowercase, keep only letters and digits
    tokens = re.sub(r'[^a-z0-9\s]', '', doc_text.lower()).split()

    dl = len(tokens)
    if dl == 0:
        continue

    # count term frequencies within this document
    tf_map = {}
    for t in tokens:
        if t:
            tf_map[t] = tf_map.get(t, 0) + 1

    for term, tf in tf_map.items():
        # term \t doc_id \t doc_title \t tf \t dl
        print('%s\t%s\t%s\t%d\t%d' % (term, doc_id, doc_title, tf, dl))
