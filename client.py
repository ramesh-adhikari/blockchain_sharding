import socket
from _thread import *
import sys
import time
from config import *
from models.sub_transaction import SubTransaction
from models.transaction import Transaction

sockets = []
leaders = {}
processing_transactions = {}
shard_id = 0
terminate_message_count = 0
current_leader_shard = 0


def handle_response_from_server(response):
    if (response.startswith("send_shard_id")):
        send_shard_id(response)
    elif response.startswith("init"):
        handle_init(response)
    elif response.startswith("check"):
        check_balance(response)
    elif response.startswith("update"):
        update_balance(response)
    elif response.startswith("commit"):
        commit_transaction(response)
    elif response.startswith("abort"):
        abort_transaction(response)
    elif response.startswith("rollback"):
        abort_rollback_transaction(response)
    elif response.startswith("release"):
        release_snapshot(response)
    elif response.startswith("terminate"):
        handle_terminate(response)


def send_shard_id(response):
    leader_shard_id = response.split(MESSAGE_DATA_SEPARATOR)[1]
    client_socket = get_socket(
        convert_shard_id_to_socket_port(leader_shard_id))
    c_host, c_port = client_socket.getsockname()
    print("Shard "+str(shard_id)+" will communicate with leader shard " +
          str(leader_shard_id) + " in port "+str(c_port))
    send_message_to_shard(leader_shard_id, "shard_id_"+str(shard_id))


def handle_init(response):
    global processing_transactions
    data = response.split(MESSAGE_DATA_SEPARATOR)
    release_all_locks_except_current_transaction(data[1], data[2])
    processing_transactions[str(data[1])] = data[2]


def check_balance(response):
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    success = Transaction.has_amount(
        sub_transaction.account_no, sub_transaction.amount)
    if success:
        if check_snapshot(sub_transaction) and check_and_apply_lock(sub_transaction, response):
            send_message_to_shard(
                sub_transaction.txn_shard_id,
                "vote_commit"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
                MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id
            )
    else:
        send_message_to_shard(
            sub_transaction.txn_shard_id, "vote_abort"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
            MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id
        )


def update_balance(response):
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    success = Transaction.has_amount(
        sub_transaction.account_no, sub_transaction.amount)
    if success:
        if check_snapshot(sub_transaction) and check_and_apply_lock(sub_transaction, response):
            Transaction.append_sub_transaction_to_temporary_file(
                sub_transaction.txn_id,
                sub_transaction.sub_txn_id,
                sub_transaction.account_no,
                sub_transaction.account_name,
                sub_transaction.amount, shard_id
            )
            send_message_to_shard(
                sub_transaction.txn_shard_id, "vote_commit"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
                MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id
            )
    else:
        send_message_to_shard(
            sub_transaction.txn_shard_id,
            "vote_abort"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
            MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id
        )


def commit_transaction(response):
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    if check_snapshot(sub_transaction):
        if (sub_transaction.type == "commit_update"):
            Transaction.move_sub_transaction_to_committed_transaction(
                shard_id,
                sub_transaction.sub_txn_id
            )
        send_message_to_shard(
            sub_transaction.txn_shard_id,
            "committed"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
            MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id
        )
        release_lock(sub_transaction.txn_shard_id, sub_transaction.account_no)


def abort_transaction(response):
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    if (sub_transaction.type == "abort_update"):
        remove_transaction(sub_transaction)
    send_message_to_shard(
        sub_transaction.txn_shard_id,
        "aborted"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
        MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id
    )
    release_snapshot(response)
    release_lock(sub_transaction.txn_shard_id, sub_transaction.account_no)


def abort_rollback_transaction(response):
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    if (sub_transaction.type == "rollback_update"):
        remove_transaction(sub_transaction)
    send_message_to_shard(
        sub_transaction.txn_shard_id,
        "rollbacked"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
        MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id
    )
    release_snapshot(response)
    release_lock(sub_transaction.txn_shard_id, sub_transaction.account_no)


def remove_transaction(sub_transaction: SubTransaction):
    if (Transaction.is_commited_transaction(shard_id, sub_transaction.sub_txn_id)):
        Transaction.remove_transaction_from_commited_transaction(
            shard_id,
            sub_transaction.sub_txn_id
        )
    elif (Transaction.is_temporary_transaction(shard_id, sub_transaction.sub_txn_id)):
        Transaction.remove_transaction_from_temporary_transaction(
            shard_id,
            sub_transaction.sub_txn_id
        )
    else:
        print("Shard "+str(shard_id)+" could not find sub-transaction "+sub_transaction.sub_txn_id +
              " in both committed and temporary transactions. Seems like it is yet to be processed.")


def handle_terminate(response):
    global terminate_message_count
    terminate_message_count += 1
    leader_shard_id = response.split(MESSAGE_DATA_SEPARATOR)[1]
    if (TRANSACTION_TYPE == 'LOCK'):
        Transaction.remove_all_account_locks_from_leader_shard(
            shard_id,
            leader_shard_id
        )


def should_terminate_client():
    return terminate_message_count == len(leaders)


def send_message_to_socket(socket, message):
    if (WRITE_LOG_TO_FILE):
        print("Shard " + str(shard_id) +
              " > Leader shard "+str(convert_socket_to_shard_id(socket))+" : "+message)
    socket.sendall(str.encode(message+MESSAGE_SEPARATOR))


def send_message_to_port(socket_port, message):
    send_message_to_socket(get_socket(socket_port), message)


def send_message_to_shard(shard, message):
    send_message_to_socket(
        get_socket(convert_shard_id_to_socket_port(shard)),
        message
    )


def convert_shard_id_to_socket_port(shard_id):
    return leaders[str(shard_id)]


def get_socket(socket_port):
    for socket in sockets:
        if (get_port_from_socket(socket) == socket_port):
            return socket
    return None


def convert_socket_to_shard_id(socket):
    port = get_port_from_socket(socket)
    for key, value in leaders.items():
        if value == port:
            return str(key)
    return "?"


def get_port_from_socket(socket):
    try:
        _host, _port = socket.getpeername()
        return _port
    except:
        time.sleep(10/1000)
        return get_port_from_socket(socket)


def check_snapshot(sub_transaction: SubTransaction):
    if (TRANSACTION_TYPE != 'OUR_PROTOCOL'):
        return True
    snapshot = Transaction.get_row_from_snapshot(
        shard_id,
        sub_transaction.account_no
    )
    if (snapshot == None):
        Transaction.append_data_to_snapshot(
            shard_id,
            sub_transaction.txn_id,
            sub_transaction.sub_txn_id,
            sub_transaction.account_no,
            sub_transaction.txn_shard_id,
            sub_transaction.txn_timestamp
        )
        return True
    elif (snapshot[2] == sub_transaction.sub_txn_id):
        return True
    else:
        snapshot_timestamp = snapshot[5]
        if (snapshot_timestamp > sub_transaction.txn_timestamp):
            send_message_to_shard(
                int(snapshot[4]),
                "vote_rollback"+MESSAGE_DATA_SEPARATOR+snapshot[1] +
                MESSAGE_DATA_SEPARATOR+snapshot[2]
            )
            Transaction.remove_snapshot(
                int(snapshot[0]),
                snapshot[2],
                snapshot[3],
            )
            Transaction.append_data_to_snapshot(
                shard_id,
                sub_transaction.txn_id,
                sub_transaction.sub_txn_id,
                sub_transaction.account_no,
                sub_transaction.txn_shard_id,
                sub_transaction.txn_timestamp
            )
            return True
        else:
            send_message_to_shard(
                sub_transaction.txn_shard_id,
                "vote_rollback"+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
                MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id
            )
            return False


def release_snapshot(response):
    if (TRANSACTION_TYPE != 'OUR_PROTOCOL'):
        return True
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    if (Transaction.snapshot_exists(shard_id, sub_transaction.account_no, sub_transaction.sub_txn_id)):
        Transaction.remove_snapshot(
            shard_id,
            sub_transaction.sub_txn_id,
            sub_transaction.account_no
        )
        print("Shard "+str(shard_id)+" released account " +
              sub_transaction.account_no)
    else:
        print("Shard "+str(shard_id)+" snapshot does not have account " +
              sub_transaction.account_no+" "+sub_transaction.sub_txn_id)


def check_and_apply_lock(sub_transaction: SubTransaction, response):
    if (TRANSACTION_TYPE != 'LOCK'):
        return True
    locked = Transaction.is_account_locked(
        shard_id,
        sub_transaction.txn_shard_id,
        sub_transaction.account_no
    )
    if (locked):
        if (str(sub_transaction.txn_shard_id) in processing_transactions):
            release_all_locks_except_current_transaction(
                sub_transaction.txn_shard_id,
                processing_transactions[str(sub_transaction.txn_shard_id)]
            )
        start_new_thread(
            retry_message_handling,
            (sub_transaction.account_no, response,)
        )
        return False
    else:
        lock_account(sub_transaction)
        return True


def lock_account(sub_transaction: SubTransaction):
    if (str(sub_transaction.txn_shard_id) in processing_transactions):
        if (sub_transaction.txn_id != processing_transactions[str(sub_transaction.txn_shard_id)]):
            print("Lock requested transaction is different from current processing transaction in shard "+str(shard_id) +
                  " from leader shard "+str(sub_transaction.txn_shard_id)+" .Ignoring lock request for "+sub_transaction.account_no)
            return
    Transaction.append_account_to_lock_file(
        shard_id,
        sub_transaction.account_no,
        sub_transaction.txn_id,
        sub_transaction.txn_shard_id,
        sub_transaction.txn_timestamp
    )
    print("Lock applied to account "+sub_transaction.account_no+" in shard " +
          str(shard_id)+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
          MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id)


def release_lock(txn_shard_id, account_number):
    if (TRANSACTION_TYPE != 'LOCK'):
        return
    if (Transaction.is_account_locked(shard_id, txn_shard_id, account_number)):
        Transaction.remove_account_lock(shard_id, account_number)
    print("Lock released from account " +
          account_number+" in shard "+str(shard_id))


def release_all_locks_except_current_transaction(txn_shard_id, txn_id):
    if (TRANSACTION_TYPE != 'LOCK'):
        return
    Transaction.remove_all_account_locks_from_leader_shard_except_current_transaction(
        shard_id,
        txn_shard_id,
        txn_id
    )
    print("All locks applied from "+str(txn_shard_id) +
          " are released in shard "+str(shard_id) + " except "+txn_id)


def retry_message_handling(account_number, response):
    if (should_terminate_client()):
        return
    sub_transaction: SubTransaction = SubTransaction.from_message(response)
    print(
        "Account "+account_number +
        " is currenlty locked in shard "+str(shard_id) +
        ". Retry request in " +
        str(ACCOUNT_LOCK_RETRY_TIME_MS)+" milliseconds." +
        " Request from "+MESSAGE_DATA_SEPARATOR+sub_transaction.txn_id +
        MESSAGE_DATA_SEPARATOR+sub_transaction.sub_txn_id
    )
    time.sleep(ACCOUNT_LOCK_RETRY_TIME_MS/1000)
    handle_response_from_server(response)


def init_client(_shard_id, _leaders):
    if (WRITE_LOG_TO_FILE):
        sys.stdout = open("logs/shard_"+str(_shard_id) + ".log", "w")

    global sockets, shard_id, leaders
    shard_id = _shard_id
    leaders = _leaders

    sockets.clear()
    for leader_shard_id, port in leaders.items():
        start_new_thread(
            connect_to_leader,
            (leader_shard_id, port,)
        )

    while not should_terminate_client():
        continue

    close_sockets()
    print("Shard "+str(shard_id) + " received terminate message from all leaders(" +
          str(len(leaders))+"). Terminating shard .....")


def connect_to_leader(leader_shard_id, port):
    client_socket = socket.socket()
    try:
        client_socket.connect((HOST, port))
    except socket.error as e:
        time.sleep(100/1000)
        connect_to_leader(leader_shard_id, port)

    sockets.append(client_socket)

    while True:
        for response in decode_response_from_server(client_socket):
            print("Leader shard "+str(leader_shard_id) +
                  " > shard "+str(shard_id)+" : "+response)
            handle_response_from_server(response)


def decode_response_from_server(client_socket):
    try:
        response_list = client_socket.recv(
            4096).decode().split(MESSAGE_SEPARATOR)
        response_list.pop()
        return response_list
    except:
        return []


def close_sockets():
    for client_socket in sockets:
        client_socket.close()
