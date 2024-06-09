import argparse
import socket
import sys
import threading

storage_dict = {}


def main_handshake(main_host: str, main_port: int, replica_port: int):
    """Replica sending command to main for connection."""
    # *3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n

    simple_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    simple_socket.connect((main_host, int(main_port)))
    PING = "*1\r\n$4\r\nPING\r\n"
    simple_socket.send(PING.encode())

    if not "PONG" in simple_socket.recv(1024).decode():
        raise Exception("Failed handshake")

    # The REPLCONF command is used to configure replication.
    simple_socket.send(
        f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{replica_port}\r\n".encode()
    )

    if not "OK" in simple_socket.recv(1024).decode():
        raise Exception("Failed handshake")

    simple_socket.send(
        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode()
    )
    if not "OK" in simple_socket.recv(1024).decode():
        raise Exception("Failed handshake")
    # The PSYNC command is used to synchronize the state of the replica with the master
    simple_socket.send("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())


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
            command = data[2].lower()
            # Order is as follows * number of strings, $ len of things for each.
            # ["*2", "$4", "ECHO", "$6", "orange", ""]
            if command == "ping":
                conn.send(b"+PONG\r\n")
            elif command == "replconf":
                conn.send("+OK\r\n".encode())

            elif command == "info":
                if data[4].lower() == "replication":
                    if is_replica:
                        conn.send("$10\r\nrole:slave\r\n".encode())
                    else:
                        response_string = "role:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0"
                        length_of_str = len(response_string)
                        conn.send(f"${length_of_str}\r\n{response_string}\r\n".encode())
            elif command == "echo":
                response = f"+{data[4]}\r\n"
                conn.send(response.encode())
            elif command == "set":
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
            elif command == "get":
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

    # NOTE: Using Main instead of Master as I prefer this.
    if args.replicaof:
        main_string_array = args.replicaof.split(" ")
        main_handshake(
            main_host=main_string_array[0],
            main_port=int(main_string_array[1]),
            replica_port=args.port,
        )

    while True:
        conn, _ = server_socket.accept()  # wait for client
        threading.Thread(
            target=handle_conn, args=(conn, True if args.replicaof else False)
        ).start()


if __name__ == "__main__":
    main()
