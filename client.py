import socket
import time
from config import HOST, MESSAGE_DATA_SEPARATOR, MESSAGE_SEPARATOR
from models.sub_transaction import SubTransaction
from models.transaction import Transaction

client_socket = None
leader_shard_id = 0
shard_id = 0


def init_client(s_id,port,leader_s_id):
    time.sleep(500 / 1000) # delaying client, so server is ready
    global client_socket, shard_id,leader_shard_id
    shard_id = s_id
    leader_shard_id = leader_s_id

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
    print("Leader shard "+str(leader_shard_id)+" > Shard "+str(shard_id)+" : "+response)
    if (response == "send_shard_id"):
        c_host, c_port = client_socket.getsockname()
        print("Shard "+str(shard_id)+" will communicate with leader shard "+str(leader_shard_id)+ " in port "+str(c_port))
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
    sub_transaction:SubTransaction = SubTransaction.from_message(response)
    success = Transaction.has_amount(sub_transaction.account_no,sub_transaction.amount)
    if success:
        send_message("vote_commit"+MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)
    else:
        send_message("vote_abort"+MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)


def update_balance(response):
    sub_transaction:SubTransaction = SubTransaction.from_message(response)
    success = Transaction.has_amount(sub_transaction.account_no,sub_transaction.amount)
    if success: 
        Transaction.append_sub_transaction_to_temporary_file(sub_transaction.txn_id,sub_transaction.sub_txn_id,sub_transaction.account_no,sub_transaction.account_name,sub_transaction.amount,shard_id)
        send_message("vote_commit"+MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)
    else:
        send_message("vote_abort"+MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)


def commit_transaction(response):
    sub_transaction:SubTransaction = SubTransaction.from_message(response)
    if(sub_transaction.type=="update"):
        Transaction.move_sub_transaction_to_committed_transaction(shard_id, sub_transaction.sub_txn_id)
    send_message("committed"+MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id) 


def abort_transaction(response):
    #TODO handle differet abort 1. insufficient balance -> transaction pool ->abort pool, sub_transaction -> remove, 2. version conflict -> transaction pool -> initial, sub_transaction -> remove
    sub_transaction:SubTransaction = SubTransaction.from_message(response)
    if(sub_transaction.type=="update"):
        Transaction.remove_transaction_from_temporary_transaction(shard_id, sub_transaction.sub_txn_id)
    send_message("aborted"+MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id) 


def send_message(message):
    client_socket.send((message+MESSAGE_SEPARATOR).encode())


def close_socket():
    global client_socket
    client_socket.close()
