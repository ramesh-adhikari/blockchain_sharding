import socket
from _thread import *
import socketserver
from config import HOST, MESSAGE_SEPARATOR, SHARDS
from models.state import State
from models.sub_transaction import split_transaction_to_sub_transactions
from models.transaction import Transaction


connections = list()
shards = {}
waiting_vote_count = 0
state: State = State.NONE
sub_transactions = []
shard_id = 0
server_socket:socketserver = None

terminate_transaction_processing = False
server_socket = None


def multi_threaded_client(connection):
    global connections
    connections.append(connection)
    send_message_to_connection(connection, "send_shard_id")

    while True:
        if terminate_transaction_processing:
            break
        for response in decode_response_from_client(connection):
            handle_response_from_client(connection, response)


def decode_response_from_client(connection):
    response_list = connection.recv(1024).decode().split(MESSAGE_SEPARATOR)
    response_list.pop()
    return response_list


def handle_response_from_client(connection, response):
    print("Shard "+convert_connection_to_shard_id(connection)+" > Leader shard "+str(shard_id)+" : "+response)
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
    if(len(shards)==len(SHARDS)):
        print("All "+str(len(SHARDS))+" shards connected to leader shard "+str(shard_id)+". Started processing transaction .....")
        process_next_transaction_in_new_thread()


def handle_vote_commit():
    global waiting_vote_count
    if (state == State.PREPARING):
        waiting_vote_count -= 1
        if (waiting_vote_count == 0):
            send_commit_message()


def handle_vote_abort():
    if (state == State.PREPARING):
        send_abort_message()


def handle_committed():
    global waiting_vote_count
    if (state == State.COMMITING):
        waiting_vote_count -= 1
        if (waiting_vote_count == 0):
            Transaction.move_transaction_from_temporary_to_committed_pool(shard_id,sub_transactions[0].txn_id)
            print(
                "Leader shard "+str(shard_id)+" received commited message from all shards for transaction "+sub_transactions[0].txn_id+". Processing next transaction .....")
            update_state(State.NONE)
            process_next_transaction_in_new_thread()


def handle_aborted():
    global waiting_vote_count
    if (state == State.ABORTING):
        waiting_vote_count -= 1
        if (waiting_vote_count == 0):
            Transaction.move_transaction_from_temporary_to_abort_pool(shard_id,sub_transactions[0].txn_id)
            print(
                "Leader shard "+str(shard_id)+" received aborted message from all shards for transaction "+sub_transactions[0].txn_id+". Processing next transaction .....")
            update_state(State.NONE)
            process_next_transaction_in_new_thread()


def send_commit_message():
    global waiting_vote_count
    update_state(State.COMMITING)
    waiting_vote_count = len(sub_transactions)
    for sub_transation in sub_transactions:
        send_message_to_port(
            convert_shard_id_to_connection_port(sub_transation.shard),
            sub_transation.change_type("commit_"+sub_transation.type).to_message()
        )


def send_abort_message():
    global waiting_vote_count
    update_state(State.ABORTING)
    waiting_vote_count = len(sub_transactions)
    for sub_transation in sub_transactions:
        send_message_to_port(
            convert_shard_id_to_connection_port(sub_transation.shard),
           sub_transation.change_type("abort_"+sub_transation.type).to_message()
        )


def convert_shard_id_to_connection_port(shard_id):
    return shards[str(shard_id)]

def convert_connection_to_shard_id(connection):
    c_host, c_port = connection.getpeername()
    for key, value in shards.items():
        if value == c_port:
            return str(key)
    return "?"


def process_next_transaction_in_new_thread(): #delay for better visibility of transactions
    start_new_thread(
        process_transaction,
        (Transaction.get_transactions_from_transaction_pool(shard_id),)
    )


def process_transaction(transaction):
    global waiting_vote_count, sub_transactions,terminate_transaction_processing
    if(transaction):
        sub_transactions = split_transaction_to_sub_transactions(transaction)
        update_state(State.PREPARING)
        waiting_vote_count = len(sub_transactions)
        for sub_transaction in sub_transactions:
            send_message_to_port(convert_shard_id_to_connection_port(
                sub_transaction.shard), sub_transaction.to_message())
    else:
        print("Leader shard "+str(shard_id)+" processed all transactions from transaction pool.")
        send_to_all_clients("end_transaction")
        terminate_transaction_processing = True
        close_server_socket()
        #TODO close all connections

def send_to_all_clients(message):
    for connection_port in shards.values():
        send_message_to_port(connection_port,message)


def send_message_to_connection(connection, message):
    connection.sendall(str.encode(message+MESSAGE_SEPARATOR))


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

def close_socket():
    server_socket.close()


def init_server(s_id,port):
    global shard_id,server_socket

    global shard_id,terminate_transaction_processing,server_socket,shards
    connected_sockets = 0

    shard_id = s_id
    server_socket = socket.socket()
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((HOST, port))
    except socket.error as e:
        print(str(e))
    server_socket.listen(len(SHARDS))

    print('Leader shard '+str(shard_id)+' connected to : ' + str(HOST) + ':' + str(port))

    while connected_sockets != len(SHARDS):
        client_socket, address = server_socket.accept()
        start_new_thread(multi_threaded_client, (client_socket, ))
        connected_sockets +=1

    while terminate_transaction_processing ==False:
            continue

def close_server_socket():
    global server_socket
    server_socket.close()

