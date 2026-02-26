"""
main1.py（保姆级教学版）

目标：用一份代码看懂 3 种网络模型，并把 Redis 的主模型放在默认路径。

你重点先看 `redis-model`：
1) 主线程用 epoll/kqueue 等待“哪个连接可读/可写”
2) 可读就收包 + 解析 RESP
3) 主线程执行命令（改内存数据）
4) 把响应写回客户端

这就是 Redis 核心思想：
- 事件驱动（很多连接）
- 命令执行尽量单线程（简单、稳定）
- 线程主要用于辅助，而不是把业务逻辑打碎到多线程
"""

import argparse
import asyncio
import selectors
import socket
import threading
from datetime import datetime, timedelta

socket_receive_buffer_size = 4096


class RedisCore:
    """
    教学用“命令核心层”：
    - 只关心命令语义（PING/SET/GET/INFO）
    - 不关心网络模型（thread-io / epoll / asyncio）
    """

    def __init__(self, role: str, thread_safe: bool = False):
        self.role = role
        self.storage = {}
        self.expire_times = {}
        self.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repl_offset = 0
        self.lock = threading.Lock() if thread_safe else None

    def handle_command(self, parts: list[str]) -> bytes:
        if self.lock is None:
            return self._handle_command_no_lock(parts)
        with self.lock:
            return self._handle_command_no_lock(parts)

    def _handle_command_no_lock(self, parts: list[str]) -> bytes:
        if not parts:
            return encode_error("empty command")

        command = parts[0].upper()

        if command == "PING":
            if len(parts) == 1:
                return encode_simple_string("PONG")
            return encode_bulk_string(parts[1])

        if command == "ECHO":
            if len(parts) != 2:
                return encode_error("wrong number of arguments for 'echo' command")
            return encode_bulk_string(parts[1])

        if command == "SET":
            if len(parts) == 3:
                key, value = parts[1], parts[2]
                self.storage[key] = value
                self.expire_times.pop(key, None)
                return encode_simple_string("OK")

            if len(parts) == 5 and parts[3].upper() in ("EX", "PX"):
                key, value = parts[1], parts[2]
                timeout_value = int(parts[4])
                self.storage[key] = value
                if parts[3].upper() == "EX":
                    self.expire_times[key] = datetime.now() + timedelta(seconds=timeout_value)
                else:
                    self.expire_times[key] = datetime.now() + timedelta(milliseconds=timeout_value)
                return encode_simple_string("OK")

            return encode_error("wrong number of arguments for 'set' command")

        if command == "GET":
            if len(parts) != 2:
                return encode_error("wrong number of arguments for 'get' command")

            key = parts[1]
            self._cleanup_expired_key(key)
            value = self.storage.get(key)
            if value is None:
                return encode_null_bulk_string()
            return encode_bulk_string(value)

        if command == "INFO":
            if len(parts) == 1 or (len(parts) == 2 and parts[1].lower() == "replication"):
                info_content = (
                    f"role:{self.role}\r\n"
                    f"master_replid:{self.master_replid}\r\n"
                    f"master_repl_offset:{self.master_repl_offset}"
                )
                return encode_bulk_string(info_content)
            return encode_bulk_string("")

        return encode_error(f"unknown command '{parts[0]}'")

    def _cleanup_expired_key(self, key: str) -> None:
        expire_time = self.expire_times.get(key)
        if expire_time is not None and datetime.now() > expire_time:
            self.storage.pop(key, None)
            self.expire_times.pop(key, None)


def encode_simple_string(value: str) -> bytes:
    return f"+{value}\r\n".encode()


def encode_bulk_string(value: str) -> bytes:
    encoded = value.encode()
    return f"${len(encoded)}\r\n".encode() + encoded + b"\r\n"


def encode_null_bulk_string() -> bytes:
    return b"$-1\r\n"


def encode_error(message: str) -> bytes:
    return f"-ERR {message}\r\n".encode()


def _find_crlf(data: bytes, start: int) -> int:
    return data.find(b"\r\n", start)


def parse_resp_from_buffer(buffer: bytes):
    if not buffer:
        return None, buffer
    if buffer[0:1] != b"*":
        raise ValueError("expected RESP array")

    index = 1
    count_end = _find_crlf(buffer, index)
    if count_end == -1:
        return None, buffer

    item_count = int(buffer[index:count_end])
    if item_count < 0:
        raise ValueError("invalid array length")

    index = count_end + 2
    items = []

    for _ in range(item_count):
        if index >= len(buffer):
            return None, buffer
        if buffer[index:index + 1] != b"$":
            raise ValueError("expected bulk string")

        index += 1
        len_end = _find_crlf(buffer, index)
        if len_end == -1:
            return None, buffer

        token_len = int(buffer[index:len_end])
        if token_len < 0:
            raise ValueError("invalid bulk string length")

        index = len_end + 2
        token_end = index + token_len
        if token_end + 2 > len(buffer):
            return None, buffer

        token = buffer[index:token_end]
        if buffer[token_end:token_end + 2] != b"\r\n":
            raise ValueError("invalid bulk string terminator")

        items.append(token.decode())
        index = token_end + 2

    return items, buffer[index:]


def read_line_blocking(connection: socket.socket) -> bytes:
    data = b""
    while not data.endswith(b"\r\n"):
        chunk = connection.recv(1)
        if chunk == b"":
            raise ConnectionError("connection closed")
        data += chunk
    return data[:-2]


def read_exact_blocking(connection: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = connection.recv(size - len(data))
        if chunk == b"":
            raise ConnectionError("connection closed")
        data += chunk
    return data


def parse_resp_array_blocking(connection: socket.socket) -> list[str]:
    first = read_exact_blocking(connection, 1)
    if first != b"*":
        raise ValueError("expected RESP array")

    item_count = int(read_line_blocking(connection))
    if item_count < 0:
        raise ValueError("invalid array length")

    items = []
    for _ in range(item_count):
        marker = read_exact_blocking(connection, 1)
        if marker != b"$":
            raise ValueError("expected bulk string")

        item_len = int(read_line_blocking(connection))
        if item_len < 0:
            raise ValueError("invalid bulk string length")

        item = read_exact_blocking(connection, item_len)
        trailer = read_exact_blocking(connection, 2)
        if trailer != b"\r\n":
            raise ValueError("invalid bulk string terminator")
        items.append(item.decode())

    return items


def serve_thread_client(connection: socket.socket, core: RedisCore) -> None:
    try:
        while True:
            try:
                request = parse_resp_array_blocking(connection)
            except ConnectionError:
                break
            except ValueError as exc:
                connection.sendall(encode_error(str(exc)))
                break

            response = core.handle_command(request)
            connection.sendall(response)
    finally:
        connection.close()


def run_thread_io_server(port: int, role: str) -> None:
    # thread-io 模式：每个客户端一个线程，I/O 与命令都在该线程里完成。
    # 好理解，但线程多时开销更高，需要锁保护共享数据。
    core = RedisCore(role=role, thread_safe=True)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("127.0.0.1", port))
    server_socket.listen()
    print("Thread-IO server started. One thread per client.")

    try:
        while True:
            conn, _ = server_socket.accept()
            thread = threading.Thread(target=serve_thread_client, args=(conn, core), daemon=True)
            thread.start()
    finally:
        server_socket.close()


def close_epoll_client(sock: socket.socket, selector, recv_buffer, send_queue):
    try:
        selector.unregister(sock)
    except Exception:
        pass
    recv_buffer.pop(sock, None)
    send_queue.pop(sock, None)
    try:
        sock.close()
    except Exception:
        pass


def run_redis_model_server(port: int, role: str) -> None:
    # ===== Redis 主模型（推荐先学这个） =====
    # - 单线程主事件循环
    # - selectors 在 Linux 下通常对应 epoll，在 BSD/macOS 常对应 kqueue
    # - 主线程负责命令执行（这点最像 Redis）
    # =======================================
    core = RedisCore(role=role, thread_safe=False)
    selector = selectors.DefaultSelector()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("127.0.0.1", port))
    server_socket.listen()
    server_socket.setblocking(False)

    recv_buffer = {}
    send_queue = {}

    selector.register(server_socket, selectors.EVENT_READ)
    print(f"Redis-model server started via selector={type(selector).__name__}")

    try:
        while True:
            events = selector.select(timeout=1)
            for key, mask in events:
                sock = key.fileobj

                # 第 1 步：新连接到来 -> accept + 注册读事件
                if sock is server_socket:
                    conn, _ = server_socket.accept()
                    conn.setblocking(False)
                    recv_buffer[conn] = b""
                    send_queue[conn] = []
                    selector.register(conn, selectors.EVENT_READ)
                    continue

                # 第 2 步：连接可读 -> 收包 + 解析 RESP
                if mask & selectors.EVENT_READ:
                    try:
                        data = sock.recv(socket_receive_buffer_size)
                    except Exception:
                        close_epoll_client(sock, selector, recv_buffer, send_queue)
                        continue

                    if not data:
                        close_epoll_client(sock, selector, recv_buffer, send_queue)
                        continue

                    recv_buffer[sock] += data
                    try:
                        while True:
                            commands, remain = parse_resp_from_buffer(recv_buffer[sock])
                            if commands is None:
                                recv_buffer[sock] = remain
                                break
                            recv_buffer[sock] = remain
                            # 第 3 步：主线程执行命令（Redis 风格关键点）
                            send_queue[sock].append(core.handle_command(commands))
                    except Exception as exc:
                        send_queue[sock].append(encode_error(str(exc)))

                    if send_queue[sock]:
                        selector.modify(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)

                # 第 4 步：连接可写 -> 回包
                if mask & selectors.EVENT_WRITE:
                    try:
                        while send_queue.get(sock):
                            sock.sendall(send_queue[sock].pop(0))
                    except Exception:
                        close_epoll_client(sock, selector, recv_buffer, send_queue)
                        continue

                    if sock in send_queue and not send_queue[sock]:
                        selector.modify(sock, selectors.EVENT_READ)
    finally:
        try:
            selector.unregister(server_socket)
        except Exception:
            pass
        selector.close()
        server_socket.close()


async def parse_resp_array_async(reader: asyncio.StreamReader) -> list[str]:
    first = await reader.readexactly(1)
    if first != b"*":
        raise ValueError("expected RESP array")

    line = await reader.readline()
    if not line.endswith(b"\r\n"):
        raise ValueError("invalid line terminator")
    item_count = int(line[:-2])
    if item_count < 0:
        raise ValueError("invalid array length")

    items = []
    for _ in range(item_count):
        marker = await reader.readexactly(1)
        if marker != b"$":
            raise ValueError("expected bulk string")

        line = await reader.readline()
        if not line.endswith(b"\r\n"):
            raise ValueError("invalid line terminator")
        item_len = int(line[:-2])
        if item_len < 0:
            raise ValueError("invalid bulk string length")

        item = await reader.readexactly(item_len)
        trailer = await reader.readexactly(2)
        if trailer != b"\r\n":
            raise ValueError("invalid bulk string terminator")
        items.append(item.decode())

    return items


async def serve_async_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, core: RedisCore) -> None:
    try:
        while True:
            try:
                request = await parse_resp_array_async(reader)
            except asyncio.IncompleteReadError:
                break
            except ConnectionError:
                break
            except ValueError as exc:
                writer.write(encode_error(str(exc)))
                await writer.drain()
                break

            writer.write(core.handle_command(request))
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def run_asyncio_server(port: int, role: str) -> None:
    # asyncio 模式：写法更现代，底层同样是事件循环 + OS 多路复用。
    core = RedisCore(role=role, thread_safe=False)
    redis_server = await asyncio.start_server(
        lambda r, w: serve_async_client(r, w, core),
        "127.0.0.1",
        port,
    )
    print("Asyncio server started.")
    async with redis_server:
        await redis_server.serve_forever()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Redis 教学版（默认 Redis 主模型）: "
            "redis-model | thread-io | asyncio"
        )
    )
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", nargs=2)
    parser.add_argument(
        "--mode",
        choices=["redis-model", "thread-io", "asyncio", "epoll"],
        default="redis-model",
        help=(
            "redis-model=Redis风格单线程事件循环(默认), "
            "thread-io=每连接一线程, asyncio=协程事件循环, epoll=redis-model别名"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    role = "slave" if args.replicaof is not None else "master"

    print(f"Starting mode={args.mode}, role={role}, port={args.port}.")
    print("教学建议：先跑 --mode redis-model，再对比 thread-io 与 asyncio。")
    print("检查复制字段：redis-cli -p <port> INFO replication")

    if args.mode == "thread-io":
        run_thread_io_server(args.port, role)
        return

    if args.mode in ("redis-model", "epoll"):
        run_redis_model_server(args.port, role)
        return

    asyncio.run(run_asyncio_server(args.port, role))


if __name__ == "__main__":
    main()