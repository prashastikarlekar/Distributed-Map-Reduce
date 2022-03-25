import json
import os
import itertools
import collections
from timeit import default_timer as timer
from multiprocessing import Process, Pool
from utils import split_input, merge_files, get_keys, join_files
from helper import inv_index_mapper,inv_index_reducer

############ WordCount implementation

class WordCount(object):    
    def __init__(self,num_mappers,num_reducers, map_func, reduce_func):
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = Pool(num_mappers)        
    
    def combine(self, mapped_values):
        merged_data = collections.defaultdict(list)
        for key, value in mapped_values:
          merged_data[key].append(value)
        # print("combined data is ",merged_data)
        with open('wc_intermediate_data.txt','w+') as f:
          f.write(str(merged_data.items()))
        # print("WORD COUNT- Intermediate data Written")
        return merged_data.items()

    def __call__(self, inputs, output_location, chunksize=6):        
        map_responses =None        
        try:          
          map_responses = self.pool.map(self.map_func, inputs, chunksize=6)
        except Exception as e:
          
          print("WORD COUNT- MAPREDUCE -- CATCH --" +e)
          
        merged_values = self.combine(itertools.chain(*map_responses))
        del map_responses  
  
        reduced_values = self.pool.map(self.reduce_func, merged_values)
        with open(output_location,'w+') as f:
          f.write(str(reduced_values))
        # del combined_data

        return True if reduced_values else False



############# InvertedIndex implementation
class InvertedIndex(object):
  def __init__(self,num_mappers, num_reducers , map_function, reduce_function, input_location, output_location) :
    self.input_location=input_location
    self.map_func =map_function
    self.reduce_func=reduce_function
    self.output_location=output_location
    self.num_mappers=num_mappers
    self.num_reducers=num_reducers
  
  def execute_map(self,index):
    chunks = open("Data/temp_"+str(self.input_location)+"/chunk_"+ str(index),'r')
    values = chunks.read()
    chunks.close()
    os.unlink("Data/temp_"+str(self.input_location)+"/chunk_"+ str(index))
    if self.map_func == "inverted_index":
            map_res = inv_index_mapper(values,index)
    else:
      map_res= self.map_func(values)

    for r in range(0,self.num_reducers):
      temp = open("Data/temp_"+str(self.input_location)+"/map_file_" + str(index)+"-" + str(r),'w+')
      json.dump(map_res,temp)
      temp.close()
    
  def execute_reduce(self,index):
        output_keys = get_keys(self.input_location)
        chunk_size = (len(output_keys))/self.num_reducers +1
        start_index = int(chunk_size*(int(index)))
        end_index = int(chunk_size*(int(index)+1))
        temp = output_keys[start_index:end_index]
        result = {}
        for element in temp:
          if self.reduce_func == "inverted_index":
            data = inv_index_reducer(element, self.input_location,self.num_mappers)
          result[element] = data
        temp_file = open("Data/temp_"+str(self.input_location)+"/reduced_file_"+str(index),'w+')
        json.dump(result,temp_file)
        temp_file.close

  def execute(self):
        split_input(self.input_location,self.num_mappers)
        map_worker = []
        reducer_worker = []
        restart_map = True
        while restart_map:
          try:
            for process_id in range(self.num_mappers):
              p = Process(target = self.execute_map ,args=(process_id,))
              p.start()
              map_worker.append(p)
            [temp.join() for temp in map_worker]
            restart_map = False
          except Exception as E:
                print(E)
        merge_files(self.input_location,self.num_mappers,self.num_reducers)
        restart_red = True
        while restart_red:
          try:
            for process_id in range(self.num_reducers):
              p = Process(target=self.execute_reduce,args=(process_id,))
              p.start()
              reducer_worker.append(p)
            [temp.join() for temp in reducer_worker]
            restart_red = False
          except:
            pass           
        join_files(self.input_location,self.num_reducers,self.output_location)
        os.unlink("Data/temp_"+str(self.input_location)+"/MergedData")
        os.rmdir("Data/temp_"+str(self.input_location)) 
        return True

