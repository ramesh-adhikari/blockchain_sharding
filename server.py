import csv
import random
import socket
from _thread import *
import time
from config import NUMBER_OF_SHARDS
from models.state import State
from models.sub_transaction import split_transaction_to_sub_transactions
from models.transaction import Transaction


host = '127.0.0.1'
port = 8085

connections = list()
shards = {}
waiting_vote_count = 0
state: State = State.NONE
sub_transactions = []


def multi_threaded_client(connection):
    global connections
    connections.append(connection)
    send_message_to_connection(connection, "send_shard_id")

    while True:
        for response in decode_response_from_client(connection):
            handle_response_from_client(connection, response)


def decode_response_from_client(connection):
    response_list = connection.recv(1024).decode().split("***")
    response_list.pop()
    return response_list


def handle_response_from_client(connection, response):
    print("Server received message : "+response)
    if (response.startswith("shard_id")):
        register_shard_id(connection, response)
    elif (response.startswith("vote_commit")):
        handle_vote_commit()
    elif (response.startswith("vote_abort")):
        handle_vote_abort()
    elif (response.startswith("committed")):
        handle_committed()
    elif (response.startswith("aborted")):
        handle_aborted()


def register_shard_id(connection, response):
    c_host, c_port = connection.getpeername()
    shards[response.replace("shard_id_", "")] = c_port


def handle_vote_commit():
    global waiting_vote_count
    if (state == State.PREPARING):
        waiting_vote_count -= 1
        if (waiting_vote_count == 0):
            send_commit_message_to_write_shards()


def handle_vote_abort():
    if (state == State.PREPARING):
        send_abort_message_to_write_shards()
    print("Handle vote abort")


def handle_committed():
    global waiting_vote_count
    if (state == State.COMMITING):
        waiting_vote_count -= 1
        if (waiting_vote_count == 0):
            print(
                "Commited message received from all parties, processing next transaction")
            update_state(State.NONE)
            process_next_transaction_in_new_thread()


def handle_aborted():
    global waiting_vote_count
    if (state == State.ABORTING):
        waiting_vote_count -= 1
        if (waiting_vote_count == 0):
            print(
                "Aborted message received from all parties, processing next transaction")
            update_state(State.NONE)
            process_next_transaction_in_new_thread()


def send_commit_message_to_write_shards():
    global waiting_vote_count
    update_state(State.COMMITING)
    waiting_vote_count = 2  # TODO send commit message to all destination shards
    for sub_transation in sub_transactions:
        if (sub_transation.type == "update"):
            send_message_to_port(
                convert_shard_id_to_connection_port(sub_transation.shard),
                "commit__"+sub_transation.txn_id
            )


def send_abort_message_to_write_shards():
    global waiting_vote_count
    update_state(State.ABORTING)
    waiting_vote_count = 2  # TODO send abort message to all destination shards
    for sub_transation in sub_transactions:
        if (sub_transation.type == "update"):
            send_message_to_port(
                convert_shard_id_to_connection_port(sub_transation.shard),
                "abort__"+sub_transation.txn_id
            )


def convert_shard_id_to_connection_port(shard_id):
    return shards[str(shard_id)]


def process_next_transaction_in_new_thread():
    start_new_thread(
        process_transaction,
        (Transaction.get_transactions_from_transaction_pool(0),) #TODO use server shard id
    )


def process_transaction(transaction):
    global waiting_vote_count, sub_transactions
    # test delay for better visibility of transaction delay
    time.sleep(50/1000)
    sub_transactions = split_transaction_to_sub_transactions(transaction)
    update_state(State.PREPARING)
    waiting_vote_count = len(sub_transactions)
    for sub_transaction in sub_transactions:
        time.sleep(100/1000)
        send_message_to_port(convert_shard_id_to_connection_port(
            sub_transaction.shard), sub_transaction.to_message())


def send_message_to_connection(connection, message):
    connection.sendall(str.encode(message+"***"))


def send_message_to_port(connection_port, message):
    send_message_to_connection(get_connection(connection_port), message)


def close_connection(connection_port):
    get_connection(connection_port).close()


def get_connection(connection_port):
    for connection in connections:
        c_host, c_port = connection.getpeername()
        if (c_port == connection_port):
            return connection
    return None


def update_state(_state: State):
    global state
    state = _state


def init_server():

    server_socket = socket.socket()
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((host, port))
    except socket.error as e:
        print(str(e))
    server_socket.listen(NUMBER_OF_SHARDS)

    process_next_transaction_in_new_thread()

    while True:
        client_socket, address = server_socket.accept()
        print('Connected to: ' + address[0] + ':' +
              str(address[1]))
        start_new_thread(multi_threaded_client, (client_socket, ))
    server_socket.close()
