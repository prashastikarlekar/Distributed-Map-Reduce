#!/usr/bin/env python3
import socket
import time
import config

print("-------TEST CASE TO RUN WORD COUNT APPLICATION-------\n")

HOST = config.HOST
PORT= config.PORT 

c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
c.connect((HOST, PORT))

# num_mappers
c.send(str.encode('5'))
time.sleep(0.01)

# num_reducers
c.send(str.encode('5'))
time.sleep(0.01)

# input_location
c.send(str.encode('myFile.txt'))
time.sleep(0.01)

# map_function
c.send(str.encode('word_count'))
time.sleep(0.01)

# reduce_ function
c.send(str.encode('word_count'))
time.sleep(0.01)

# output_file_location
c.send(str.encode('word_count_output.txt'))
time.sleep(0.01)

res  = c.recv(1024)
time.sleep(0.01)
print("Server response:")
print(res.decode('utf-8'))
c.close()
