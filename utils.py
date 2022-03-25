import os
import re
import json

def split_input(input_location,num_mappers):
        file_size = os.path.getsize("Data/"+input_location)
        section = file_size/num_mappers + 1
        input_file = open("Data/"+input_location,'r', encoding='utf-8')
        parts = input_file.read()
        parts = (re.sub('[^A-Za-z]+',' ', parts))
        input_file.close()
        (index,split) = (1,1)
        if not (os.path.exists("Data/temp_"+str(input_location))):
            os.mkdir('Data/temp_'+str(input_location))
            
        curr_split_section = open("Data/temp_"+str(input_location)+"/chunk_"+str(split-1),"w+")

        for char in parts:
            curr_split_section.write(char)
            if (index>section*split+1) and (char.isspace()):
                curr_split_section.close()
                split += 1
                curr_split_section = open("Data/temp_"+str(input_location)+"/chunk_"+str(split-1),"w+")
            index += 1
        curr_split_section.close()

def merge_files(input_location,num_mappers,num_reducers):
        merged_data = []

        for i in range(0,num_mappers):
            for j in range(0,num_reducers):
                file_path="Data/temp_"+str(input_location)+"/map_file_" + str(i)+"-" + str(j)
                temp_file = open("Data/temp_"+str(input_location)+"/map_file_" + str(i)+"-" + str(j),'r')

                if not os.path.getsize(file_path) == 0:
                    merged_data.append(json.load(temp_file))
                temp_file.close()
                os.unlink("Data/temp_"+str(input_location)+"/map_file_" + str(i)+"-" + str(j))
        merged_file = open("Data/temp_"+str(input_location)+"/MergedData","w")
        json.dump(merged_data,merged_file)

def get_keys(input_location):
        temp = open("Data/temp_"+str(input_location)+"/MergedData",'r')
        map_response = json.load(temp)
        output_keys = set()
        for temp_list in map_response:
            for key in temp_list.keys():
                if len(key) > 2:
                    output_keys.add(key)
        output_keys = list(output_keys)
        output_keys.sort()
        return output_keys

def join_files(input_location,num_reducers,output_location):
        temp_list = []
        for index in range(0,num_reducers):
            r_file = open("Data/temp_"+str(input_location)+"/reduced_file_"+str(index),'r')
            temp_list.append(json.load(r_file))
            r_file.close()
            os.unlink("Data/temp_"+str(input_location)+"/reduced_file_"+str(index))
        output_file = open(output_location,'w+')
        op_dict = {}
        for dict in temp_list:
            op_dict.update(dict)
        json.dump(op_dict,output_file)
        output_file.close()

if __name__=="__main__":
    pass