import sys

# DTU's HPC won't install mrjob. Cloned repo and placed it locally
sys.path.insert(0, '../mrjob')
from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.compat

import os

data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../data')

class AverageOutDegree(MRJob):

    def mapper(self, _, filename):
        with open(os.path.join(data_path, 'links', filename), mode='r', encoding='utf-8') as f:
            yield 'lines', sum(1 for _ in f)

    def reducer(self, _, value):
        nodes = int(mrjob.compat.jobconf_from_env('nodes'))

        yield 'Average degree', sum(value) / nodes

names_file = os.path.join(data_path, 'test_names.txt')
nodes = sum(1 for _ in open(names_file))

job = AverageOutDegree(args=[names_file, '--jobconf', 'nodes=' + str(nodes)])
with job.make_runner() as runner:
    runner.run()
    with open('average_degree.txt', 'w', encoding='utf-8') as f:
        for index, row in job.parse_output(runner.cat_output()):
            f.write(index + ': ' + str(row) + '\n')
