import socket
import time
import sys
import config 

print("-------TEST CASE TO CONNECT MULTIPLE CLIENTS TO THE SERVER-------\n")

c1 = socket.socket()
c2 = socket.socket()
HOST = config.HOST
PORT = config.PORT

print("Connecting to Client 1")
c1.connect((HOST, PORT))
print("Connecting to Client 2")
c2.connect((HOST, PORT))

c1.send(str.encode('4'))
c2.send(str.encode('4'))
time.sleep(0.01)
c1.send(str.encode('7'))
c2.send(str.encode('7'))
time.sleep(0.01)
c1.send(str.encode('myFile.txt'))
c2.send(str.encode('myFile.txt'))
time.sleep(0.01)
c1.send(str.encode('word_count'))
c2.send(str.encode('inverted_index'))
time.sleep(0.01)
c1.send(str.encode('word_count'))
c2.send(str.encode('inverted_index'))
time.sleep(0.01)
c1.send(str.encode('word_count_output.txt'))
c2.send(str.encode('inverted_index_output.txt'))
time.sleep(0.01)
res1 = c1.recv(1024)
res2 = c2.recv(1024)
time.sleep(0.01)
print("Response from Client 1:")
print(res1.decode('utf-8'))
print("-----------Execution of Client 1 request is completed.-----------")
print("Response from Client 2:")
print(res2.decode('utf-8'))
print("-----------Execution of Client 2 request is completed.-----------")

c1.close()
c2.close()