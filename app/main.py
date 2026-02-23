import socket  # noqa: F401
import threading  # noqa: F401

thread_lock = threading.Lock()
def process_command(command: str) -> str:
    i =0
    command_token =[]
    if command.startswith('*'):
        i += 1
        if command[i].isdigit():
            number = 0
            while i < len(command) and command[i].isdigit():
                number = number*10 + int(command[i])
                i+=1
            if i < len(command) and command[i] == '\r' and i+1 < len(command) and command[i+1] == '\n':
                i+=2
                for _ in range(number):
                    if i < len(command) and command[i] == '$':
                        i+=1
                        length = 0
                        while i < len(command) and command[i].isdigit():
                            length = length*10 + int(command[i])
                            i+=1
                        if i < len(command) and command[i] == '\r' and i+1 < len(command) and command[i+1] == '\n':
                            i+=2
                            if i + length <= len(command):
                                token = command[i:i+length]
                                command_token.append(token)
                                i+=length
                                if i < len(command) and command[i] == '\r' and i+1 < len(command) and command[i+1] == '\n':
                                    i+=2
                                else:
                                    return "ERR invalid bulk string terminator"
                            else:
                                return "ERR bulk string length exceeds command length"
                        else:
                            return "ERR invalid bulk string length terminator"
                    else:
                        return "ERR expected bulk string"
    return command_token

def handle_client(client_socket, client_address):

    try:
        with client_socket:
            while True:
                # 接收客户端数据
                data = client_socket.recv(1024)
                if not data:
                    break
                commands = process_command(data.decode())
                if not commands:
                    client_socket.sendall(b"-ERR empty command\r\n")
                    continue
                if commands[0] == "PING":
                    client_socket.sendall(b"+PONG\r\n")
                elif commands[0] == "ECHO":
                    if len(commands) != 2:
                        client_socket.sendall(b"-ERR wrong number of arguments for 'echo' command\r\n")
                    else:
                        response = f"${len(commands[1])}\r\n{commands[1]}\r\n"
                        client_socket.sendall(response.encode())
                elif commands[0] == "SET":
                    if len(commands) != 3:
                        client_socket.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")
                    else:
                        key = commands[1]
                        value = commands[2]
                        with thread_lock:
                            storage[key] = value
                        client_socket.sendall(b"+OK\r\n")
                elif commands[0] == "GET":
                    if len(commands) != 2:
                        client_socket.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
                    else:
                        key = commands[1]
                        with thread_lock:
                            value = storage.get(key)
                        if value is None:
                            client_socket.sendall(b"$-1\r\n")
                        else:
                            response = f"${len(value)}\r\n{value}\r\n"
                            client_socket.sendall(response.encode()) 
    except ConnectionResetError:
        print(f"客户端 {client_address} 异常断开")
    except Exception as e:
        print(f"处理客户端 {client_address} 时出错: {e}")
    finally:
        print(f"线程结束: {client_address}")

def main():
    global storage
    storage = {}
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
