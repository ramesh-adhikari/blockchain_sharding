import socket
from _thread import *
import socketserver
import sys
import time
from config import *
from models.state import State
from models.sub_transaction import split_transaction_to_sub_transactions
from models.transaction import Transaction


client_sockets = list()
shards = {}
waiting_vote_count = 0
state: State = State.NONE
sub_transactions = []
shard_id = 0
server_socket: socketserver = None

terminate_transaction_processing = False
server_socket = None


def handle_response_from_client(socket, response):
    print("Shard "+convert_socket_to_shard_id(socket) +
          " > Leader shard "+str(shard_id)+" : "+response)
    if (response.startswith("shard_id")):
        register_shard_id(socket, response)
        return

    if (not check_if_current_transaction(response) and sub_transactions):
        print("Leader shard "+str(shard_id)+" is currently processing transaction " +
              sub_transactions[0].txn_id+". So received message is ignored. "+response)
        return
    if (response.startswith("vote_commit")):
        handle_vote_commit()
    elif (response.startswith("vote_abort")):
        handle_vote_abort()
    elif (response.startswith("committed")):
        handle_committed()
    elif (response.startswith("aborted")):
        handle_aborted()
    elif (response.startswith("vote_rollback")):
        handle_vote_rollback()
    elif (response.startswith("rollbacked")):
        handle_rollbacked()


def register_shard_id(socket, response):
    shards[response.replace("shard_id_", "")] = get_port_from_socket(socket)
    if (len(shards) == len(SHARDS)):
        print("All "+str(len(SHARDS))+" shards connected to leader shard " +
              str(shard_id)+". Started processing transaction .....")
        process_next_transaction_in_new_thread()


def handle_vote_commit():
    global waiting_vote_count
    if (state == State.PREPARING):
        waiting_vote_count -= 1
        if (waiting_vote_count == 0):
            send_commit_message()


def send_commit_message():
    global waiting_vote_count
    update_state(State.COMMITING)
    waiting_vote_count = len(sub_transactions)
    for sub_transation in sub_transactions:
        send_message_to_shard(
            sub_transation.shard_id,
            sub_transation.change_type(
                "commit_"+sub_transation.type).to_message()
        )


def handle_vote_abort():
    if (state == State.PREPARING):
        send_abort_message()


def send_abort_message():
    global waiting_vote_count
    update_state(State.ABORTING)
    waiting_vote_count = len(sub_transactions)
    for sub_transation in sub_transactions:
        send_message_to_shard(
            sub_transation.shard_id,
            sub_transation.change_type(
                "abort_"+sub_transation.type).to_message()
        )


def handle_committed():
    global waiting_vote_count
    if (state == State.COMMITING):
        waiting_vote_count -= 1
        if (waiting_vote_count == 0):
            Transaction.move_transaction_from_temporary_to_committed_pool(
                shard_id, sub_transactions[0].txn_id)
            print(
                "Leader shard "+str(shard_id)+" received commited message from all shards for transaction "+sub_transactions[0].txn_id+". Processing next transaction .....")
            update_state(State.NONE)
            send_release_message()
            process_next_transaction_in_new_thread()


def handle_aborted():
    global waiting_vote_count
    if (state == State.ABORTING):
        waiting_vote_count -= 1
        if (waiting_vote_count == 0):
            Transaction.move_transaction_from_temporary_to_abort_pool(
                shard_id, sub_transactions[0].txn_id)
            print(
                "Leader shard "+str(shard_id)+" received aborted message from all shards for transaction "+sub_transactions[0].txn_id+". Processing next transaction .....")
            update_state(State.NONE)
            send_release_message()
            process_next_transaction_in_new_thread()


def handle_vote_rollback():
    if (state == State.PREPARING or state == State.COMMITING):
        send_rollback_message()


def send_rollback_message():
    global waiting_vote_count
    update_state(State.ROLLBACKING)
    waiting_vote_count = len(sub_transactions)
    for sub_transation in sub_transactions:
        send_message_to_shard(
            sub_transation.shard_id,
            sub_transation.change_type(
                "rollback_"+sub_transation.type).to_message()
        )


def handle_rollbacked():
    global waiting_vote_count
    if (state == State.ROLLBACKING):
        waiting_vote_count -= 1
        if (waiting_vote_count == 0):
            Transaction.move_transaction_from_temporary_to_initial_pool(
                shard_id, sub_transactions[0].txn_id)
            print(
                "Leader shard "+str(shard_id)+" received abort rollbacked message from all shards for transaction "+sub_transactions[0].txn_id+". Processing next transaction .....")
            update_state(State.NONE)
            send_release_message()
            process_next_transaction_in_new_thread()


def send_release_message():
    if (TRANSACTION_TYPE != 'OUR_PROTOCOL'):
        return
    for sub_transation in sub_transactions:
        send_message_to_shard(
            sub_transation.shard_id,
            sub_transation.change_type("release").to_message()
        )


def check_if_current_transaction(response):
    return (sub_transactions and sub_transactions[0].txn_id in response)


def update_state(_state: State):
    global state
    state = _state


def process_next_transaction_in_new_thread():
    start_new_thread(
        process_transaction,
        (Transaction.get_transactions_from_transaction_pool(shard_id),)
    )


def process_transaction(transaction):
    global waiting_vote_count, sub_transactions, terminate_transaction_processing
    if (transaction):
        print("Leader shard "+str(shard_id) +
              " processing transaction "+str(transaction))
        sub_transactions = split_transaction_to_sub_transactions(
            transaction,
            shard_id
        )
        update_state(State.PREPARING)
        waiting_vote_count = len(sub_transactions)
        for sub_transaction in sub_transactions:
            send_message_to_shard(
                sub_transaction.shard_id,
                "init_transaction"+MESSAGE_DATA_SEPARATOR +
                str(shard_id)+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id
            )
            send_message_to_shard(
                sub_transaction.shard_id,
                sub_transaction.to_message()
            )
    else:
        print("Leader shard "+str(shard_id) +
              " processed all transactions from transaction pool.")
        send_message_to_all_clients(
            "terminate"+MESSAGE_DATA_SEPARATOR+str(shard_id))
        terminate_transaction_processing = True
        close_server_socket()


def send_message_to_all_clients(message):
    print("Leader shard "+str(shard_id) +
          " sending terminate message to all shards.")
    for socket_port in shards.values():
        send_message_to_port(socket_port, message)


def send_message_to_shard(shard, message):
    send_message_to_socket(
        get_socket(convert_shard_id_to_socket_port(shard)),
        message
    )


def send_message_to_socket(socket, message):
    if (WRITE_LOG_TO_FILE):
        print("Leader shard "+str(shard_id) +
              " > Shard "+convert_socket_to_shard_id(socket)+" : "+message)
    socket.sendall(str.encode(message+MESSAGE_SEPARATOR))


def send_message_to_port(socket_port, message):
    send_message_to_socket(get_socket(socket_port), message)


def get_socket(socket_port):
    for socket in client_sockets:
        if (get_port_from_socket(socket) == socket_port):
            return socket
    return None


def convert_shard_id_to_socket_port(shard_id):
    return shards[str(shard_id)]


def convert_socket_to_shard_id(socket):
    c_port = get_port_from_socket(socket)
    for key, value in shards.items():
        if value == c_port:
            return str(key)
    return "?"


def get_port_from_socket(socket):
    try:
        _host, _port = socket.getpeername()
        return _port
    except:
        time.sleep(1/1000)
        return get_port_from_socket(socket)


def init_server(_shard_id, port):
    if (WRITE_LOG_TO_FILE):
        sys.stdout = open("logs/leader_shard_"+str(_shard_id) + ".log", "w")

    global shard_id, terminate_transaction_processing, server_socket, shards
    connected_sockets = 0
    shard_id = _shard_id
    server_socket = socket.socket()
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((HOST, port))
    except socket.error as e:
        print(str(e))
    server_socket.listen(len(SHARDS))

    print('Leader shard '+str(shard_id) +
          ' connected to : ' + str(HOST) + ':' + str(port))

    while connected_sockets != len(SHARDS):
        client_socket, address = server_socket.accept()
        start_new_thread(accept_new_client, (client_socket, ))
        connected_sockets += 1

    while terminate_transaction_processing == False:
        continue


def accept_new_client(socket):
    global client_sockets
    client_sockets.append(socket)
    send_message_to_socket(
        socket,
        "send_shard_id" + MESSAGE_DATA_SEPARATOR+str(shard_id)
    )

    while terminate_transaction_processing == False:
        for response in decode_response_from_client(socket):
            handle_response_from_client(socket, response)


def decode_response_from_client(socket):
    try:
        response_list = socket.recv(4096).decode().split(MESSAGE_SEPARATOR)
        response_list.pop()
        return response_list
    except:
        return []


def close_server_socket():
    global server_socket
    server_socket.close()
