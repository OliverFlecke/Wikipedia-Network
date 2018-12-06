import re
import sys

max_distance = 0 
longest = []

path = sys.argv[1]

with open(path, 'r', encoding='utf-8') as f:
    for line in f:
        m = re.search('(?P<id>\d*),\((?P<distance>\d*), \[(?P<path>.*)\], (?P<state>[0-2])\)', line) 
        
        distance = int(m.group('distance'))
        id = int(m.group('id'))
        path = [int(x) for x in m.group('path').split(',')]

        if distance == sys.maxsize:
            continue

        if distance > max_distance:
            max_distance = distance
            longest = [(id, path)]
        elif distance == max_distance:
            longest.append((id, path))

print('Longest distance to any node: %d' % (max_distance))
print(longest[0])

