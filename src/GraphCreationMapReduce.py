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

        row = set()
        name = name.replace("'","\'")

        row.add(name)

        with open(main_path+"links/"+name,"r",encoding="utf-8") as file:
            for line in file:
                if(line is not ""):
                    link_encoded = get_filename(line)
                    row.add(link_encoded)

        #with open(main_path + name, "w+", encoding="utf-8") as file:
            #file.write("debug")

        yield index,list(row)

    def steps(self):
        return [MRStep(mapper=self.mapper)]


        






#name,number
