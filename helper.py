import os
import string
import json
import multiprocessing

STOP_WORDS = set([
            'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in', 
            'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with', 'on'
            ])
    
TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

######## WORD COUNT

def map_func(filename, file_dir = 'Data'):   # map function for word count    
    output = []
    file_path = os.path.join(file_dir,filename)
    with open(file_path, 'rt',encoding= "utf8") as f:
        for line in f:
            line = line.translate(TR)  # Strip punctuation
            for word in line.split():
                word = word.lower()
                if word.isalpha() and word not in STOP_WORDS and len(word) > 1:
                    output.append((word, 1))
    return output


def reduce_func(item):      # reduce function for word count
    word, counts = item
    return (word, sum(counts))

######## INVERTED INDEX

def inv_index_mapper(value,index):
        result = {}
        words = value.split()
        for word in words:
            temp_list = [index,1]
            word = word.lower()
            if word in result.keys():
                result[word][1] += 1
            else:
                result[word] = temp_list
        return result

def inv_index_reducer(key,input_location,num_mappers):
        temp = open("Data/temp_"+str(input_location)+"/MergedData",'r')
        map_res = json.load(temp)
        temp.close()
        temp = []
        for lis in map_res:
            if key in lis.keys():
                temp.append(lis[key])
        result = []
        for i in range(num_mappers):
            count = 0
            for ele in temp:
                if ele[0] == i:
                    count += ele[1]
            if count == 0:
                pass
            else:
                result.append([i,count])
        return result

















# OLDDDDDDDDDDDDDDDDDDDDDDDDDDDD


# def mapper(inputs,file_dir='Data'):
#     print("INVERTED INDEX  - in mapper")
#     print("INVERTED INDEX  - inputs is",inputs)
    
#     # for record in inputs:
#         # file_path = os.path.join(inputs,record)
#     # print("record is ", record)
#     mapper_output=[]
#     file_path = os.path.join(file_dir,inputs)
#     print(file_path)

#     with open((file_path), 'rt',encoding= "utf8") as f:
#         for line in f:
#             # print("word is ",word)
#             # key, value = line.split('\t', 1)
#             # for word in value.strip().split():
#             line = line.translate(TR)  # Strip punctuation
#             for word in line.split():
#                 word = word.lower()
#                 if word.isalpha() and word not in STOP_WORDS and len(word) > 1 and (word,inputs,count) not in mapper_output:
#                     if (word)
#                     mapper_output.append((word, inputs,count))

#     print("INVERTED INDEX  - mapper_output is ",mapper_output)
#     return mapper_output






# def reducer(record):
#     print("INVERTED INDEX  - in reducer")

#     key   = None
#     total = 0
#     reducer_output=[]
#     for line in record:
#         k, v  = line.split('\t', 1)
#         count = int(v.strip())

#         if key == k:
#             total += count
#         else:
#             if key:
#                 reducer_output.append(('{}\t{}').format(key, total))
#             key   = k
#             total = count
#     if key:
#         reducer_output.append(('{}\t{}').format(key, total))
#     return reducer_output

# def mapper(record,file_dir = 'Data'):
#     # key: document identifier
#     # value: document contents
#     key = record[0]
#     value = record[1]
#     words = value.split()
#     output=[]
#     for w in words:
#     #   mr.emit_intermediate(w, key)
#         output.append((w,key))
#     return output

# def reducer(key, list_of_values):
#     # key: word
#     # value: list of occurrence counts
#     total = {}
#     for v in list_of_values:
#         if not total.has_key(v):
#             total[v] = v
#     return ((key,total.keys()))