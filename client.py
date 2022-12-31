import socket
import time
from config import HOST, MESSAGE_DATA_SEPARATOR, MESSAGE_SEPARATOR
from models.transaction import Transaction

client_socket = None
shard_id = 0
terminate_transaction_processing = False


def init_client(s_id,port):
    time.sleep(50 / 1000)
    global client_socket, shard_id
    shard_id = s_id

    client_socket = socket.socket()
    client_socket.connect((HOST, port))

    while True:
        if terminate_transaction_processing:
            break
        for response in decode_response_from_server(client_socket):
            handle_response_from_server(response)

    close_socket()

def decode_response_from_server(client_socket):
    response_list = client_socket.recv(1024).decode().split(MESSAGE_SEPARATOR)
    response_list.pop()
    return response_list


def handle_response_from_server(response):
    global terminate_transaction_processing
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
    elif response.startswith("end_transaction"):
        terminate_transaction_processing = True


def check_balance(response):
    command = response.split(MESSAGE_DATA_SEPARATOR)
    success = Transaction.has_amount(command[1],command[2])
    if success:
        send_message("vote_commit"+MESSAGE_DATA_SEPARATOR+response)
    else:
        send_message("vote_abort"+MESSAGE_DATA_SEPARATOR+response)


def update_balance(response):
    command = response.split(MESSAGE_DATA_SEPARATOR)
    success = Transaction.has_amount(command[1],command[2]) #TODO implement update call
    if success: 
        Transaction.append_sub_transaction_to_temporary_file(command[3],command[4],command[1],command[1].split('_')[1],command[2],shard_id)
        send_message("vote_commit"+MESSAGE_DATA_SEPARATOR+response)
    else:
        send_message("vote_abort"+MESSAGE_DATA_SEPARATOR+response)


def commit_transaction(response):
    command = response.split(MESSAGE_DATA_SEPARATOR)
    if(command[3]=="update"):
        Transaction.move_sub_transaction_to_committed_transaction(shard_id, command[2])
    send_message("committed"+MESSAGE_DATA_SEPARATOR+response)  # TODO commit transction


def abort_transaction(response):
    #TODO handle differet abort 1. insufficient balance -> transaction pool ->abort pool, sub_transaction -> remove, 2. version conflict -> transaction pool -> initial, sub_transaction -> remove
    command = response.split(MESSAGE_DATA_SEPARATOR)
    if(command[3]=="update"):
        Transaction.remove_transaction_from_temporary_transaction(shard_id, command[2])
    send_message("aborted"+MESSAGE_DATA_SEPARATOR+response)  # TODO abort transction


def send_message(message):
    client_socket.send((message+MESSAGE_SEPARATOR).encode())


def close_socket():
    global client_socket
    client_socket.close()
