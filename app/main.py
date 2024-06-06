import socket
import threading

storage_dict = {}


def handle_conn(conn):
    with conn:
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            # "*1\r\n$4\r\nPING"
            # response = b"+PONG\r\n"
            data = data.split("\r\n")
            # Order is as follows * number of strings, $ len of things for each.
            # ["*2", "$4", "ECHO", "$6", "orange", ""]
            if data[2].lower() == "ping":
                conn.send(b"+PONG\r\n")
            elif data[2].lower() == "echo":
                response = f"+{data[4]}\r\n"
                conn.send(response.encode())
            elif data[2].lower() == "set":
                storage_dict[data[4]] = data[6]
                conn.send(b"+OK\r\n")
            elif data[2].lower() == "get":
                return_value = storage_dict[data[4]]
                conn.send(f"${len(return_value)}\r\n{return_value}\r\n".encode())
            # else:
            #     conn.send(response)


def main():

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()  # wait for client
        threading.Thread(target=handle_conn, args=(conn,)).start()


if __name__ == "__main__":
    main()
