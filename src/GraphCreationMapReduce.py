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

class GraphCounter(MRJob):
    def mapper(self,_,line):
        yield (None,1)

    def reducer(self,_,count):
        yield (None,sum(count))

class WikipediaGraph(MRJob):

    def mapper(self, _, line):
        index,name = line.split(",")[:2]

        row = [0]*int(mrjob.compat.jobconf_from_env('nodes'))

        name = name.replace("'","\'")
        with open(main_path+"links/"+name,"r",encoding="utf-8") as file:
            for line in file:
                if(line is not ""):
                    link_encoded = get_filename(line)
                    row[self.get_index_of_file(link_encoded)] = 1

        yield index,row

    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def get_index_of_file(self, filename: str) -> int:
        with open(main_path+"index_file", "r", encoding="utf-8") as file:
            for line in file:
                linesplit = line.split(",")
                if(linesplit[0] == filename):
                    return int(linesplit[1])

            return -1
        
        






#name,number
