import socket
import threading

"""
# TODO: Parse RESP.
# TODO: Support for ECHO command (Encoded as RESP bulk string). No hardcoding.
# TODO: Implement redis protocol parser.
# TODO: Input which will be encoded using RESP.

"""


def handle_conn(conn):
    with conn:
        while True:
            data = conn.recv(1024).decode()
            # "*1\r\n$4\r\nPING"
            response = b"+PONG\r\n"
            data = data.split("\r\n")
            ["*2", "$4", "ECHO", "$6", "orange", ""]
            if data[2].lower() == "echo":
                conn.send(f"+{data[4]}\r\n".encode())
            else:
                conn.send(response)


def main():

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()  # wait for client
        threading.Thread(target=handle_conn, args=(conn,)).start()


if __name__ == "__main__":
    main()
