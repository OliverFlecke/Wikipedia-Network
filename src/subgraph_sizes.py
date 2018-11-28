import os
from util import get_root
from collections import Counter
import re

regex = r'(?P<key>\d*),\((?P<subgraph_no>\d+), (?P<state>[0-2])\)'

start_node = 0

with open(os.path.join(get_root(), 'data', 'graph_file'), 'r', encoding='utf-8') as file:
    for line in file:
        start_node = int(line.split(':')[0])

directory = os.path.realpath(os.path.join(get_root(), 'output/subgraphs', str(start_node)))

last_round = 0

subgraph_counter = Counter()

with open(os.path.join(directory, str(last_round)), 'w', encoding='utf-8') as f:
    for line in f:
        m = re.match(regex, str(line))
        subgraph_no = int(m.group('subgraph_no'))
        subgraph_counter.update(subgraph_no)
