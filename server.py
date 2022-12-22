import socket
from _thread import *

connections = list()
host = '127.0.0.1'
port = 8085


def multi_threaded_client(connection):
    global connections
    connections.append(connection)
    connection.send(str.encode('Server is working:'))
    while True:
        data = connection.recv(2048)
        if not data:
            break
        response = 'Client: ' + data.decode('utf-8')
        print(response)
        send_message(len(connections)-1,
                     "Acknowledge for message : " + response)
    close_connection(len(connections)-1)


def send_message(i, message):
    connections[i].sendall(str.encode(message))


def close_connection(i):
    connections[0].close()


def init_server():
    ServerSideSocket = socket.socket()
    ServerSideSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        ServerSideSocket.bind((host, port))
    except socket.error as e:
        print(str(e))
    print('Socket is listening..')
    ServerSideSocket.listen(5)

    while True:
        Client, address = ServerSideSocket.accept()
        print('Connected to: ' + address[0] + ':' + str(address[1]))
        start_new_thread(multi_threaded_client, (Client, ))
    ServerSideSocket.close()
