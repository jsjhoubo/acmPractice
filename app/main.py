import socket  # noqa: F401
import threading  # noqa: F401

def handle_client(client_socket, client_address):
    try:
        with client_socket:
            while True:
                # 接收客户端数据
                data = client_socket.recv(1024)
                if not data:
                    break
                
                if data == b"PING\r\n" or data == b"*1\r\n$4\r\nPING\r\n":
                    client_socket.sendall(b"+PONG\r\n")
    except ConnectionResetError:
        print(f"客户端 {client_address} 异常断开")
    except Exception as e:
        print(f"处理客户端 {client_address} 时出错: {e}")
    finally:
        print(f"线程结束: {client_address}")

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("127.0.0.1", 6379), reuse_port=True)

    while True:
        client_socket, client_address = server_socket.accept()
        client_thread = threading.Thread(
            target=handle_client,
            args=(client_socket, client_address)
        )
        
        # 设置为守护线程，这样主线程结束时子线程也会自动结束
        client_thread.daemon = True
        
        # 启动线程
        client_thread.start()


if __name__ == "__main__":
    main()
