import sys
import os

root = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')

# DTU's HPC won't install mrjob. Cloned repo and placed it locally
sys.path.insert(0, os.path.join(root, 'mrjob'))
from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.compat
from collections import Counter
from util import get_filename
import json

data_path = os.path.join(root, 'data')

class AverageOutDegree(MRJob):

    def mapper(self, _, page):
        with open(os.path.join(data_path, 'links', get_filename(page)), mode='r', encoding='utf-8') as f:
            yield 'linsk', sum(1 for _ in f)


    def reducer(self, key, values):
        yield 'degrees', dict(Counter(values))

names_file = os.path.join(data_path, 'pages.txt')
nodes = sum(1 for _ in open(names_file))

job = AverageOutDegree(args=[names_file, '--jobconf', 'nodes=' + str(nodes)])
with job.make_runner() as runner:
    runner.run()
    with open('degrees.json', 'w', encoding='utf-8') as f:
        for index, row in job.parse_output(runner.cat_output()):
            json.dump(row, f)
