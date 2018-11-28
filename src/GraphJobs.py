from GraphCreationMapReduce import *
import json

def create_index_file():
    import os
    dir = os.listdir("../data/links")
    with open("../data/index_file","w+",encoding="utf-8") as file:
        file.writelines([str(i)+","+str(dir[i])+"\n" for i in range(len(dir))])

def get_node_count() -> int:
    mr_job = GraphCounter(args=["../data/index_file"])
    with mr_job.make_runner() as runner:
        runner.run()
        for key,value in mr_job.parse_output(runner.cat_output()):
            return value

def create_and_store_graph():
    import tqdm

    #Clear file
    with open("../data/graph_file", "w+", encoding="utf-8") as file:
        file.write("")

    mr_job = WikipediaGraph(args=["../data/pages.txt"])
    with mr_job.make_runner() as runner:
        runner.run()
        for index,row in mr_job.parse_output(runner.cat_output()):
            with open("../data/graph_file","a",encoding="utf-8") as file:
                file.write(f'{index}:{row}\n') 

if __name__ == '__main__':
    #create_index_file()
    create_and_store_graph()
