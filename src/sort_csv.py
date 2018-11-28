import csv
import os 
import sys
from util import get_root

csv_filename = sys.argv[1]

with open(os.path.join(get_root(), 'output', csv_filename), 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter='|')
    
    sorted_list = sorted(reader, key=lambda x: int(x[1]), reverse=True)

with open(os.path.join(get_root(), 'output', f'sorted_{csv_filename}'), 'w', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='|')
    for row in sorted_list:
        writer.writerow(row)
