import argparse
import socket
import sys
import threading

storage_dict = {}


def delete_key(delete_key: str):
    del storage_dict[delete_key]


def handle_conn(conn, is_replica):
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
            elif data[2].lower() == "info":
                if data[4].lower() == "replication":
                    if is_replica:
                        conn.send("$10\r\nrole:slave\r\n".encode())
                    else:
                        response_string = "role:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0"
                        length_of_str = len(response_string)
                        conn.send(f"${length_of_str}\r\n{response_string}\r\n".encode())
            elif data[2].lower() == "echo":
                response = f"+{data[4]}\r\n"
                conn.send(response.encode())
            elif data[2].lower() == "set":
                key = data[4]
                storage_dict[key] = data[6]
                if len(data) > 8:
                    if data[8].lower() == "px":
                        delete_timer = data[10]
                        threading.Timer(
                            interval=float(delete_timer) / 1000.0,
                            function=delete_key,
                            args=[key],
                        ).start()
                conn.send(b"+OK\r\n")
            elif data[2].lower() == "get":
                if data[4].lower() in storage_dict:
                    return_value = storage_dict[data[4]]
                    conn.send(f"${len(return_value)}\r\n{return_value}\r\n".encode())
                else:
                    conn.send(b"$-1\r\n")
            # else:
            #     conn.send(response)


def main():
    parser = argparse.ArgumentParser(description="redis app")
    parser.add_argument("--port", action="store", dest="port", default=6379)
    parser.add_argument("--replicaof", action="store", dest="replicaof")
    args = parser.parse_args()
    server_socket = socket.create_server(("localhost", int(args.port)), reuse_port=True)
    # TODO: Need to create a handshake between master and slave.

    if args.replicaof:
        main_string_array = args.replicaof.split(" ")
        main_host = main_string_array[0]
        main_port = main_string_array[1]
        simple_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        simple_socket.connect((main_host, int(main_port)))
        simple_socket.send("*1\r\n$4\r\nPING\r\n".encode())
    while True:
        conn, _ = server_socket.accept()  # wait for client
        threading.Thread(
            target=handle_conn, args=(conn, True if args.replicaof else False)
        ).start()


if __name__ == "__main__":
    main()
