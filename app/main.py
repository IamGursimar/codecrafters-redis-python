import socket
import threading


def handle_conn(conn):
    with conn:
        while True:
            data = conn.recv(1024)
            conn.send(b"+PONG\r\n")


def main():

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()  # wait for client
        threading.Thread(target=handle_conn, args=(conn,)).start()


if __name__ == "__main__":
    main()
