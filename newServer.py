import socket
from master import main
from _thread import *

def threaded_client(connection):
    print("SERVER -- PROCESSING THREAD")

    num_mappers = int(connection.recv(2048).decode('utf-8'))
    num_reducers = int(connection.recv(2048).decode('utf-8'))
    input_location = connection.recv(2048).decode('utf-8')
    map_func = connection.recv(2048).decode('utf-8')
    red_func = connection.recv(2048).decode('utf-8')
    output_location = connection.recv(2048).decode('utf-8')

        
    res = main(num_mappers,num_reducers,map_func,red_func,input_location,output_location)
    #currently we only support fix functions
    res = "Server -- "+ str(res)
    res = res.encode()
    # reply = 'Server Says: ' + data.decode('utf-8')
    connection.send(res)            
        
    print("SERVER ----Task completed")
    connection.close()


if __name__ == "__main__":
    HOST = "localhost"  # Standard loopback interface address (localhost)
    PORT = 9889        # Port to listen on (non-privileged ports are > 1023)
    # PORT = int(sys.argv[1])
    myServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    try:
        myServer.bind((HOST, PORT))
    except socket.error as e:
        print(str(e))

    print('Waiting for a Connection..')
    myServer.listen(5)



    ThreadCount = 0

    while True:
        Client, address = myServer.accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        start_new_thread(threaded_client, (Client, ))
        # time.sleep(15)
        ThreadCount += 1
        print('Thread Number: ' + str(ThreadCount))
    myServer.close()

