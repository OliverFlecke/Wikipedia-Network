import sys
import os
from util import get_filename,get_root

# DTU's HPC won't install mrjob. Cloned repo and placed it locally
sys.path.insert(0, os.path.join(get_root(), 'mrjob'))
from mrjob.job import MRJob


data_path = os.path.join(get_root(), 'data')

class DegreeIn(MRJob):

    def mapper(self, _, page):
        with open(os.path.join(data_path, 'links', get_filename(page)), mode='r', encoding='utf-8') as f:
            for line in f:
                yield line.rstrip('\n'), 1

    def reducer(self, key, values):
        yield key, sum(values)

names_file = os.path.join(data_path, 'pages.txt')
nodes = sum(1 for _ in open(names_file))

job = DegreeIn(args=[names_file, '--jobconf', 'nodes=' + str(nodes)])
with job.make_runner() as runner:
    runner.run()
    with open(os.path.join(get_root(), 'output', 'degree_in.csv'), 'w', encoding='utf-8') as f:
        for index, row in job.parse_output(runner.cat_output()):
            f.write(index + '|' + str(row) + '\n')
