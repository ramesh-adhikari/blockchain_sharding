import socket
import time
from server import host, port

client_socket = None

shard_id = 0


def init_client(s_id):
    time.sleep(50 / 1000)
    global client_socket, shard_id
    shard_id = s_id

    client_socket = socket.socket()
    client_socket.connect((host, port))

    while True:
        response = client_socket.recv(1024).decode()
        handle_response_from_server(response)


def handle_response_from_server(response):
    print("Client "+str(shard_id)+" received message : "+response)
    if (response == "send_shard_id"):
        send_message("shard_id_"+str(shard_id))
    elif "__check__" in response:
        account = response.split("__")
        check_balance(account[0], account[2])
    elif "__update__" in response:
        account = response.split("__")
        update_balance(account[0], account[2])


def check_balance(account_no, amount):
    print("Check balalnce : Account "+account_no+", Amount "+str(amount))


def update_balance(account_no, amount):
    print("Update balalnce : Account "+account_no+", Amount "+str(amount))


def send_message(message):
    global client_socket
    client_socket.send(message.encode())


def close_socket():
    global client_socket
    client_socket.close()
