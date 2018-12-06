import sys
import os

root = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')

# DTU's HPC won't install mrjob. Cloned repo and placed it locally
sys.path.insert(0, os.path.join(root, 'mrjob'))
from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.compat
from util import get_filename, get_root
import re
from datetime import datetime

UNVISITED = 0
FRONTIER = 1
VISITED = 2

indexes = {}

with open(os.path.join(get_root(), 'data', 'pages.txt'), 'r', encoding='utf-8') as f:
    counter = 0
    for line in f:
        indexes[counter] = line.rstrip('\n')
        counter += 1

neighbors = {}

with open(os.path.join(get_root(), 'data', 'graph_file'), 'r', encoding='utf-8') as file:
    for line in file:
        key, values = line.split(':')
        values = values[1:-3]
        if values == '':
            neighbors[int(key)] = []
        else:
            neighbors[int(key)] = [int(x.strip()) for x in values.split(',')]

regex = r'(?P<key>\d*),\((?P<dist>\d+), \[(?P<path>.*)\], (?P<state>[0-2])\)'

class ShortestPath(MRJob):

    def mapper(self, key, value):
        m = re.match(regex, str(value))
        key = int(m.group('key'))
        dist = int(m.group('dist'))
        path = [int(x) for x in m.group('path').split(', ')]
        state = int(m.group('state'))

        if state == FRONTIER:
            if key in neighbors:
                for neighbor in neighbors[key]:
                    yield neighbor, (dist + 1, path + [neighbor], FRONTIER)

            yield key, (dist, path, VISITED)
        else:
            yield key, (dist, path, state)

    def reducer(self, key, lines):
        min_dist = sys.maxsize
        min_path = []
        min_state = 0
        
        for value in lines:
            try:
                dist, path, state = value
            except:
                start = value.index('(')
                end = value.index(')')
                dist, path, state = str(value)[start + 1:end].split(', ')

                dist = int(dist)
                path = [int(x.strip()) for x in path[1:-1].split(',')]
                state = int(state)

            if dist <= min_dist:
                min_dist = dist
                min_path = path

            if state > min_state:
                min_state = state
 
        yield key, (min_dist, min_path, min_state) 

# (key, (distance, path, state))

def find_shortest_path(start_node: int):
    nodes = {
        index: (sys.maxsize, [index], UNVISITED)
        for index in neighbors.keys()
    }
    nodes[start_node] = (0, [start_node], FRONTIER)

    round = 0
    directory = os.path.realpath(os.path.join(get_root(), 'output/shortest', str(start_node)))
    if not os.path.exists(directory):
        os.mkdir(directory)

    with open(os.path.join(directory, str(round)), 'w', encoding='utf-8') as file:
        for key, value in nodes.items():
            file.write(f'{key},{value}\n')

    current = 0
    print(f'Searching from node {start_node}')
    print(f'Start {datetime.now()}')
    while True:
        done = True
        current_file = os.path.join(directory, str(round))
        print(current_file)
        job = ShortestPath(args=[current_file])
        with job.make_runner() as runner:
            runner.run()
            round += 1
            with open(os.path.join(directory, str(round)), 'w', encoding='utf-8') as f:
                for key, row in job.parse_output(runner.cat_output()):
                    dist, path, state = row
                    f.write(f'{key},({dist}, {path}, {state})\n')
                    if state == UNVISITED or state == FRONTIER:
                        done = False
                    if state == FRONTIER:
                        current += 1

        print(f'{round} completed')
        
        if current == 0:
            print('No more nodes on the frontier, stopping')
            break

        current = 0

        if done:
            break

    print(f'Done {datetime.now()}')

node = int(sys.argv[1])
find_shortest_path(node)
