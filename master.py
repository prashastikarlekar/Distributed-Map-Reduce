import os
from MapReduce import WordCount,InvertedIndex
from helper import map_func, reduce_func

def main(num_mappers,num_reducers,map_function, reduce_function, input_location,output_location):
    if map_function =="word_count" :
        file  = os.listdir("Data")

        try:
            mapped = WordCount(num_mappers,num_reducers, map_func, reduce_func)
            status = mapped(file,output_location)
            
            if status:
                res= "Task completed."
            
            return res
        except Exception as e:
            return e

    elif map_function == "inverted_index":

        obj = InvertedIndex(num_mappers, num_reducers, map_function, reduce_function, input_location, output_location)
        status= obj.execute()
        if status:
            res="Task completed."
            
        return res

if __name__=="__main__":
    main(5,6,"word_count","word_count","myFile.txt","wc-output.txt")
    # obj.execute()
    main()