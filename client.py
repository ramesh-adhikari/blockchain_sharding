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
        for response in decode_response_from_server(client_socket):
            handle_response_from_server(response)


def decode_response_from_server(client_socket):
    response_list = client_socket.recv(1024).decode().split("***")
    response_list.pop()
    return response_list


def handle_response_from_server(response):
    print("Client "+str(shard_id)+" received message : "+response)
    if (response == "send_shard_id"):
        send_message("shard_id_"+str(shard_id))
    elif response.startswith("check__"):
        check_balance(response)
    elif response.startswith("update__"):
        update_balance(response)
    elif response.startswith("commit__"):
        commit_transaction(response)
    elif response.startswith("abort__"):
        abort_transaction(response)


def check_balance(response):
    # TODO check balance here, send vote_abort if insufficient balance
    send_message("vote_commit__"+response)


def update_balance(response):
    # TODO First check balance here, send vote_abort if insufficient balance else update balance and send vote_commit
    send_message("vote_commit__"+response)


def commit_transaction(response):
    send_message("committed__"+response)  # TODO commit transction


def abort_transaction(response):
    send_message("aborted__"+response)  # TODO abort transction


def send_message(message):
    client_socket.send((message+"***").encode())


def close_socket():
    global client_socket
    client_socket.close()
