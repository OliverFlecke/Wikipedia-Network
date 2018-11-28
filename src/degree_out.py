import sys
import os
from util import get_filename, get_root

# DTU's HPC won't install mrjob. Cloned repo and placed it locally
sys.path.insert(0, os.path.join(get_root(), 'mrjob'))
from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.compat

data_path = os.path.join(get_root(), 'data')

class DegreeOut(MRJob):

    def mapper(self, _, page):
        with open(os.path.join(data_path, 'links', get_filename(page)), mode='r', encoding='utf-8') as f:
            yield page, sum(1 for line in f)

names_file = os.path.join(data_path, 'pages.txt')

job = DegreeOut(args=[names_file])
with job.make_runner() as runner:
    runner.run()
    with open(os.path.join(get_root(), 'output', 'degree_out.csv'), 'w', encoding='utf-8') as f:
        for index, row in job.parse_output(runner.cat_output()):
            f.write(index + '|' + str(row) + '\n')
