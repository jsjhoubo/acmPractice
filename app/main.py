import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("127.0.0.1", 6379), reuse_port=True)

    while True:
        connection, _ = server_socket.accept()
        with connection:
            data = connection.recv(1024)
            if not data:
                continue

            if data == b"PING\r\n" or data == b"*1\r\n$4\r\nPING\r\n":
                connection.sendall(b"+PONG\r\n")
                continue

        connection.close()


if __name__ == "__main__":
    main()
