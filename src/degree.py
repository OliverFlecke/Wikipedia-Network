import sys
import os

root = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')

# DTU's HPC won't install mrjob. Cloned repo and placed it locally
sys.path.insert(0, os.path.join(root, 'mrjob'))
from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.compat
from util import get_filename

data_path = os.path.join(root, 'data')

class AverageOutDegree(MRJob):

    def mapper(self, _, page):
        with open(os.path.join(data_path, 'links', get_filename(page)), mode='r', encoding='utf-8') as f:
            links = sum(1 for _ in f)

            yield 'degree', links
            yield 'max', links
            yield 'min', links

    def reducer(self, key, values):

        if key == 'degree':
            nodes = int(mrjob.compat.jobconf_from_env('nodes'))
            total_links = sum(list(values))

            yield 'Total number of links', total_links
            yield 'Average degree', total_links / nodes
        
        elif key == 'max':
            yield 'Min degree', min(list(values))
        elif key == 'min':
            yield 'Max degree', max(list(values))

names_file = os.path.join(data_path, 'pages.txt')
nodes = sum(1 for _ in open(names_file))

job = AverageOutDegree(args=[names_file, '--jobconf', 'nodes=' + str(nodes)])
with job.make_runner() as runner:
    runner.run()
    with open('degree_statistics.txt', 'w', encoding='utf-8') as f:
        for index, row in job.parse_output(runner.cat_output()):
            f.write(index + ': ' + str(row) + '\n')
