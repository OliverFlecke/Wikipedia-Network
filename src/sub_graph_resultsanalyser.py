import os
from collections import Counter

root = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')



with open(os.path.join(root, 'output', 'subgraphs', '161598', 'data'), 'r', encoding='utf-8') as f:
    clusters = []
    for line in f:
        clusterNo = line.split(",")[1][1:]
        clusters.append(clusterNo)

counter = Counter(clusters)
print(counter)