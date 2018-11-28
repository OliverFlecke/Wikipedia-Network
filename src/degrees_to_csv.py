import json
import os
import sys
from util import get_root

filename = sys.argv[1]

with open(os.path.join(get_root(), 'output', filename), 'r', encoding='utf-8') as f:
    degrees = json.load(f)

with open(os.path.join(get_root(), 'output', os.path.splitext(filename)[0] + '.csv'), 'w', encoding='utf-8') as f:
    f.write('x,y\n')
    for key, value in sorted(degrees.items(), key=lambda x: int(x[0])):
        f.write(f'{key},{value}\n')
