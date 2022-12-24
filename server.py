import csv
import socket
from _thread import *
import time

from models.sub_transaction import split_transaction_to_sub_transactions


host = '127.0.0.1'
port = 8085

connections = list()
shards = {}

server_shard_id = 0


def multi_threaded_client(connection):
    global connections
    connections.append(connection)
    send_message_to_connection(connection, "send_shard_id")

    while True:
        data = connection.recv(2048)
        if not data:
            break
        response = data.decode('utf-8')
        handle_response_from_client(connection, response)
    connection.close()


def handle_response_from_client(connection, response):
    print("Server received message : "+response)
    if (response.startswith("shard_id")):
        c_host, c_port = connection.getpeername()
        shards[response.replace("shard_id_", "")] = c_port


def convert_shard_id_to_connection_port(shard_id):
    return shards[str(shard_id)]


def process_transaction_from_transaction_pool():
    # waiting for clients to connect
    time.sleep(1)
    for transaction in get_transactions_from_transaction_pool():
        process_transaction(transaction)
        # adding delay beteween each transcation for better visibility in testing
        time.sleep(50/1000)


def process_transaction(transaction):
    for sub_transaction in split_transaction_to_sub_transactions(transaction):
        if (sub_transaction.shard == server_shard_id):
            print("Server & destination same")
        else:
            send_message_to_port(convert_shard_id_to_connection_port(
                sub_transaction.shard), sub_transaction.to_message())


def get_transactions_from_transaction_pool():
    data = []
    with open('datas/transactions.csv', encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        for row in csvReader:
            data.append(row)
    return data


def send_message_to_connection(connection, message):
    connection.sendall(str.encode(message))


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


def init_server(shard_id):
    global server_shard_id
    server_shard_id = shard_id

    server_socket = socket.socket()
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((host, port))
    except socket.error as e:
        print(str(e))
    print('Socket is listening..')
    server_socket.listen(5)

    start_new_thread(process_transaction_from_transaction_pool, ())

    while True:
        client_socket, address = server_socket.accept()
        print('Connected to: ' + address[0] + ':' +
              str(address[1]))
        start_new_thread(multi_threaded_client, (client_socket, ))
    ServerSideSocket.close()
