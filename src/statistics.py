import os
import sys
from util import get_root
import csv

filename = sys.argv[1]

with open(os.path.join(get_root(), 'output', filename), 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter='|')

    total_links = 0
    count = 0
    min_degree = sys.maxsize
    max_degree = -1

    for _, degree in reader:
        degree = int(degree)
        total_links += degree
        count += 1

        if degree < min_degree:
            min_degree = degree
        if degree > max_degree:
            max_degree = degree

with open(os.path.join(get_root(), 'output', f'statistics_{filename}'), 'w', encoding='utf-8') as f:
    f.write(f'Total pages: {count}\n')
    f.write(f'Total links: {total_links}\n')
    f.write(f'Average degree: {total_links / count}\n')
    f.write(f'Max degree: {max_degree}\n')
    f.write(f'Min degree: {min_degree}\n')

