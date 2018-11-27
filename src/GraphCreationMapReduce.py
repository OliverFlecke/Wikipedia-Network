import sys
import os
from util import get_filename

# DTU's HPC won't install mrjob. Cloned repo and placed it locally
if os.path.isdir('../mrjob'):
    sys.path.insert(0, '../mrjob')

from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.compat

main_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'../data/')

indexes = {}

with open(os.path.join(main_path, 'pages.txt'), 'r', encoding='utf-8') as f:
    counter = 0
    for line in f:
        indexes[line.rstrip('\n')] = counter
        counter += 1

class GraphCounter(MRJob):
    def mapper(self,_,line):
        yield (None,1)

    def reducer(self,_,count):
        yield (None,sum(count))

class WikipediaGraph(MRJob):

    def mapper(self, _, name):
        row = set()
        filename = get_filename(name)

        with open(main_path+"links/" + filename,"r",encoding="utf-8") as file:
            for line in file:
                line = line.rstrip()
                if line in indexes:
                    row.add(indexes[line])

        yield indexes[name], list(row)

    def steps(self):
        return [MRStep(mapper=self.mapper)]

#name,number
