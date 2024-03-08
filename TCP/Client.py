import socket
import struct


def send_large_data(conn, data):
    msg_length = len(data)
    header = struct.pack("!Q", msg_length)
    conn.send(header)

    chunk_size = 1024
    for i in range(0, msg_length, chunk_size):
        chunk = data[i:i + chunk_size]
        conn.send(chunk)

    # Signal the end of data by sending an empty chunk
    conn.send(b'')


def receive_large_data(conn):
    header = conn.recv(8)
    if not header:
        return None

    message_length = struct.unpack("!Q", header)[0]
    received_data = bytearray()

    while len(received_data) < message_length:
        chunk = conn.recv(min(1024, message_length - len(received_data)))
        if not chunk:
            # Connection closed by the remote host
            raise ConnectionResetError("Connection closed by the remote host")
        received_data.extend(chunk)

    return received_data.decode()


def receive_and_reconstruct_data(conn):
    full_data = ""
    while True:
        chunk = receive_large_data(conn)
        if chunk == "END_OF_DATA":
            break
        full_data += chunk
    return full_data

def client_program():
    host = socket.gethostname()
    port = 5050
    client_socket = socket.socket()
    client_socket.connect((host, port))
    print("Welcome to the Database Client")
    while True:
        print("\nAvailable Commands:")
        print("1. CREATE DATABASE <db_name>")
        print("2. USE <db_name>")
        print("3. CREATE TABLE <table_name> ( attributes )")
        print("4. CREATE INDEX <index_name> ON <table_name> ( <column_names> )")
        print("5. DROP DATABASE <db_name>")
        print("6. DROP TABLE <table_name>")
        print("7. STOP to exit")
        print("8. INSERT INTO <table_name> values (values)")
        print('9. DELETE from <table_name> where primaryKey= <value>')
        command = input("Enter a command: ")

        if command.lower() == "stop":
            send_large_data(client_socket, command.encode())
            break
        if command.split()[0].lower() == 'select':
            send_large_data(client_socket, command.encode())
            full_data = receive_and_reconstruct_data(client_socket)
            print("Received from server: " + str(full_data))

        else:
            send_large_data(client_socket, command.encode())
            data = client_socket.recv(65536).decode()
            print("Received from server: " + str(data))
    client_socket.close()


if __name__ == '__main__':
    client_program()
