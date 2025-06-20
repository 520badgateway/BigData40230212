#!/home/hadoop/anaconda3/bin/python
# -*- coding: utf-8 -*-
import sys

current_key = None
current_count = 0

for line in sys.stdin:
    key, value = line.strip().split('\t', 1)
    try:
        count = int(value)
    except:
        continue

    if key == current_key:
        current_count += count
    else:
        if current_key:
            print(f"{current_key}\t{current_count}")
        current_key = key
        current_count = count

if current_key:
    print(f"{current_key}\t{current_count}")
