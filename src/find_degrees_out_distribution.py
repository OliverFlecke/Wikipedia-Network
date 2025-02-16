import sys
import os
from util import get_filename, get_root

# DTU's HPC won't install mrjob. Cloned repo and placed it locally
sys.path.insert(0, os.path.join(get_root(), 'mrjob'))
from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.compat
from collections import Counter
import csv

data_path = os.path.join(get_root(), 'data')

class AverageOutDegree(MRJob):

    def mapper(self, _, page):
        with open(os.path.join(data_path, 'links', get_filename(page)), mode='r', encoding='utf-8') as f:
            yield 'link', sum(1 for _ in f)

    def reducer(self, link, values):
        yield 'degrees', dict(Counter(values))

names_file = os.path.join(data_path, 'pages.txt')

job = AverageOutDegree(args=[names_file])
with job.make_runner() as runner:
    runner.run()
    with open(os.path.join(get_root(), 'output', 'degrees_out_frequency.csv'), 'w', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',')
        for index, degrees in job.parse_output(runner.cat_output()):
            writer.writerow(('Degree', 'Count')
            for row in sorted(degrees.items(), key=lambda x: int(x[0])):
                writer.writerow(row)

                
