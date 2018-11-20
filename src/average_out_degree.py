import sys

# DTU's HPC won't install mrjob. Cloned repo and placed it locally
sys.path.insert(0, '../mrjob')
from mrjob.job import MRJob
from mrjob.step import MRStep

import os

data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../data/links')

class AverageOutDegree(MRJob):
    
    def mapper(self, _, filename):
        with open(os.path.join(data_path, filename), mode='r', encoding='utf-8') as f:
            yield 'lines', sum(1 for _ in f)
    
    def reducer(self, _, value):
        yield 'Average degree', sum(value)

job = AverageOutDegree()
job.run()
