import matplotlib.pyplot as plt
import json
import os
import numpy as np
from util import get_root

with open(os.path.join(get_root(), 'output', 'degrees.json'), 'r', encoding='utf-8') as f:
    degrees = json.load(f)

xs = [int(x) for x in degrees.keys()]

fig, ax = plt.subplots(1, 1, figsize=(20, 10))
ax.scatter(xs, degrees.values())
ax.set_xlabel('Degree')
ax.set_ylabel('Frequency')
ax.set_title('Degree distribution')
ax.set_xticks(np.arange(0, max(xs), step=200))

fig.savefig('degree_distrbution.png')

fig, ax = plt.subplots(1, 1, figsize=(20, 10))
ax.loglog(xs, degrees.values(), 'o')
ax.set_xlabel('Degree')
ax.set_ylabel('Frequency')
ax.set_title('Loglog Degree distribution')

fig.savefig('degree_distrbution_loglog.png')
