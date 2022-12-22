import socket
import time
from server import host, port

client_socket = None


def init_client(client_no):
    global client_socket

    client_socket = socket.socket()
    client_socket.connect((host, port))

    while True:
        time.sleep(2)
        send_message("HELLO from client "+str(client_no) +
                     " on "+str(time.time()))
        data = client_socket.recv(1024).decode()
        print('Server: ' + data)


def send_message(message):
    global client_socket
    client_socket.send(message.encode())


def close_socket():
    global client_socket
    client_socket.close()
