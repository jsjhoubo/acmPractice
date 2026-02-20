import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("127.0.0.1", 6379), reuse_port=True)

    while True:
        connection, _ = server_socket.accept()
        connection.close()


if __name__ == "__main__":
    main()
