import socket
import time
from config import HOST, MESSAGE_DATA_SEPARATOR, MESSAGE_SEPARATOR
from models.transaction import Transaction

client_socket = None
shard_id = 0


def init_client(s_id,port):
    time.sleep(50 / 1000)
    global client_socket, shard_id
    shard_id = s_id

    client_socket = socket.socket()
    client_socket.connect((HOST, port))

    while True:
        for response in decode_response_from_server(client_socket):
            handle_response_from_server(response)


def decode_response_from_server(client_socket):
    response_list = client_socket.recv(1024).decode().split(MESSAGE_SEPARATOR)
    response_list.pop()
    return response_list


def handle_response_from_server(response):
    print("Client "+str(shard_id)+" received message : "+response)
    if (response == "send_shard_id"):
        send_message("shard_id_"+str(shard_id))
    elif response.startswith("check"):
        check_balance(response)
    elif response.startswith("update"):
        update_balance(response)
    elif response.startswith("commit"):
        commit_transaction(response)
    elif response.startswith("abort"):
        abort_transaction(response)


def check_balance(response):
    command = response.split(MESSAGE_DATA_SEPARATOR)
    success = Transaction.has_amount(command[1],command[2])
    if success:
        send_message("vote_commit__"+response)
    else:
        send_message("vote_abort__"+response)


def update_balance(response):
    command = response.split(MESSAGE_DATA_SEPARATOR)
    success = Transaction.has_amount(command[1],command[2]) #TODO implement update call
    # if success: 
        # Transaction.append_temporary_transaction()
    # send_message("vote_commit__"+response)


def commit_transaction(response):
    # command = response.split(MESSAGE_DATA_SEPARATOR)
    print(response)
    exit()
    Transaction.move_sub_transaction_to_committed_transaction(shard_id, command[2])
    # Transaction.move_transaction_from_temporary_to_committed_pool(shard_id, command[1])
    send_message("committed__"+response)  # TODO commit transction


def abort_transaction(response):
    command = response.split(MESSAGE_DATA_SEPARATOR)
    Transaction.remove_transaction_from_temporary_transaction(shard_id, command[2])
    Transaction.move_transaction_from_temporary_to_abort_pool(shard_id, command[1])
    send_message("aborted__"+response)  # TODO abort transction


def send_message(message):
    client_socket.send((message+MESSAGE_SEPARATOR).encode())


def close_socket():
    global client_socket
    client_socket.close()
