import select
import socket  # noqa: F401
from datetime import datetime, timedelta

socket_queue_size = 128
socket_receive_buffer_size = 1024
socket_timeout = 1
transaction_queue_limit = 1000


class RedisStream:
    def __init__(self):
        self.entries = []


class BlockedXReadRequest:
    def __init__(self, client_socket, resolved_streams, count, deadline):
        self.client_socket = client_socket
        self.resolved_streams = resolved_streams
        self.count = count
        self.deadline = deadline
        self.stream_keys = {stream_key for stream_key, _ in resolved_streams}


def parse_stream_id(entry_id: str):
    parts = entry_id.split("-")
    if len(parts) != 2:
        return None

    milliseconds, sequence = parts
    if not milliseconds.isdigit() or not sequence.isdigit():
        return None

    return int(milliseconds), int(sequence)


def resolve_xread_stream_offsets(streams_to_read, resolve_dollar_to_latest):
    resolved_streams = []
    for stream_key, last_id in streams_to_read:
        if last_id == "$":
            if resolve_dollar_to_latest:
                if stream_key in storage and isinstance(storage[stream_key], RedisStream) and storage[stream_key].entries:
                    latest_entry_id = storage[stream_key].entries[-1][0]
                    latest_entry_tuple = parse_stream_id(latest_entry_id)
                    if latest_entry_tuple is None:
                        return None, b"-ERR Invalid stream ID specified as stream command argument\r\n"
                    resolved_streams.append((stream_key, latest_entry_tuple))
                else:
                    resolved_streams.append((stream_key, (0, 0)))
            else:
                resolved_streams.append((stream_key, None))
        elif last_id == "0":
            resolved_streams.append((stream_key, (0, 0)))
        else:
            parsed_last_id = parse_stream_id(last_id)
            if parsed_last_id is None:
                return None, b"-ERR Invalid stream ID specified as stream command argument\r\n"
            resolved_streams.append((stream_key, parsed_last_id))

    return resolved_streams, None


def collect_xread_matching_entries(resolved_streams, count):
    result_streams = []
    for stream_key, last_id_tuple in resolved_streams:
        if stream_key not in storage or not isinstance(storage[stream_key], RedisStream):
            continue

        if last_id_tuple is None:
            continue

        matching_entries = []
        for entry_id, fields in storage[stream_key].entries:
            parsed_entry_id = parse_stream_id(entry_id)
            if parsed_entry_id is not None and parsed_entry_id > last_id_tuple:
                matching_entries.append((entry_id, fields))

        if count is not None and count >= 0:
            matching_entries = matching_entries[:count]

        if matching_entries:
            result_streams.append((stream_key, matching_entries))

    return result_streams


def encode_xread_response(result_streams):
    response = f"*{len(result_streams)}\r\n".encode()
    for stream_key, matching_entries in result_streams:
        response += f"*2\r\n${len(stream_key)}\r\n{stream_key}\r\n".encode()
        response += f"*{len(matching_entries)}\r\n".encode()
        for entry_id, fields in matching_entries:
            response += f"*2\r\n${len(entry_id)}\r\n{entry_id}\r\n".encode()
            response += f"*{len(fields)}\r\n".encode()
            for field in fields:
                response += f"${len(field)}\r\n{field}\r\n".encode()
    return response


def wake_blocked_xread_clients_for_stream(stream_key: str):
    for request in list(blocked_xread_requests):
        if stream_key not in request.stream_keys:
            continue

        client = request.client_socket
        if client not in send_queue:
            blocked_xread_requests.remove(request)
            continue

        result_streams = collect_xread_matching_entries(
            request.resolved_streams,
            request.count,
        )
        if not result_streams:
            continue

        send_queue[client].append(encode_xread_response(result_streams))
        if client not in outputs:
            outputs.append(client)
        blocked_xread_requests.remove(request)

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

    try:
        item_count = int(buffer[index:count_end])
    except ValueError as exc:
        raise ValueError("invalid array length") from exc

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

        try:
            token_len = int(buffer[index:len_end])
        except ValueError as exc:
            raise ValueError("invalid bulk string length") from exc

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


def wake_blocked_clients_for_key(key: str, max_wake_count: int):
    if key not in blocked_clients_by_key:
        return
    if key not in storage or not isinstance(storage[key], list):
        return
    if max_wake_count <= 0:
        return

    waiting_clients = blocked_clients_by_key[key]
    remaining_wake_count = max_wake_count
    while waiting_clients and storage[key] and remaining_wake_count > 0:
        client = waiting_clients.pop(0)
        if client not in send_queue:
            blocked_client_deadline.pop(client, None)
            continue

        value = storage[key].pop(0)
        send_queue[client].append(
            f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n".encode()
        )
        blocked_client_deadline.pop(client, None)
        if client not in outputs:
            outputs.append(client)
        remaining_wake_count -= 1

    if not waiting_clients:
        blocked_clients_by_key.pop(key, None)


def handle_client(commands, client_socket=None):
    response = b""
    try:
        if not commands:
            return b"-ERR empty command\r\n"

        if commands[0] == "PING":
            response = b"+PONG\r\n"
        elif commands[0] == "ECHO":
            if len(commands) != 2:
                response = b"-ERR wrong number of arguments for 'echo' command\r\n"
            else:
                response = f"${len(commands[1])}\r\n{commands[1]}\r\n".encode()
        elif commands[0] == "SET":
            if len(commands) == 3:
                key = commands[1]
                value = commands[2]
                storage[key] = value
                response = b"+OK\r\n"
            elif len(commands) == 5 and (commands[3].upper() == "EX" or commands[3].upper() == "PX"):
                key = commands[1]
                value = commands[2]
                timeout_value = int(commands[4])
                storage[key] = value
                if commands[3].upper() == "EX":
                    expire_time = datetime.now() + timedelta(seconds=timeout_value)
                elif commands[3].upper() == "PX":
                    expire_time = datetime.now() + timedelta(milliseconds=timeout_value)
                expire_times[key] = expire_time
                response = b"+OK\r\n"
            else:
                response = b"-ERR wrong number of arguments for 'set' command\r\n"
        elif commands[0] == "GET":
            if len(commands) != 2:
                response = b"-ERR wrong number of arguments for 'get' command\r\n"
            else:
                key = commands[1]
                value = storage.get(key)
                expire_time = expire_times.get(key)
                if expire_time and datetime.now() > expire_time:
                    del storage[key]
                    del expire_times[key]
                    value = None
                if value is None:
                    response = b"$-1\r\n"
                else:
                    response = f"${len(value)}\r\n{value}\r\n".encode()
        elif commands[0] == "INCR":
            if len(commands) != 2:
                response = b"-ERR wrong number of arguments for 'incr' command\r\n"
            else:
                key = commands[1]
                value = storage.get(key)

                if value is None:
                    value = "1"
                    storage[key] = value
                    response = b":1\r\n"
                else:
                    try:
                        incremented_value = int(value) + 1
                    except ValueError:
                        response = b"-ERR value is not an integer or out of range\r\n"
                    else:
                        storage[key] = str(incremented_value)
                        response = f":{incremented_value}\r\n".encode()
        elif commands[0] == "RPUSH":
            key = commands[1]
            if key not in storage:
                storage[key] = []
            elif not isinstance(storage[key], list):
                response = b"-ERR wrong type of value for 'rpush' command\r\n"
                return response
            for value in commands[2:]:
                storage[key].append(value)
            pushed_length = len(storage[key])
            wake_blocked_clients_for_key(key, len(commands) - 2)
            response = f":{pushed_length}\r\n".encode()
        elif commands[0] == "LPUSH":
            key = commands[1]
            if key not in storage:
                storage[key] = []
            elif not isinstance(storage[key], list):
                response = b"-ERR wrong type of value for 'lpush' command\r\n"
                return response
            for value in commands[2:]:
                storage[key].insert(0, value)
            pushed_length = len(storage[key])
            wake_blocked_clients_for_key(key, len(commands) - 2)
            response = f":{pushed_length}\r\n".encode()
        elif commands[0] == "LRANGE":
            key = commands[1]
            if key not in storage or not isinstance(storage[key], list):
                response = b"*0\r\n"
            else:
                try:
                    start = int(commands[2])
                    end = int(commands[3])
                    if end < 0:
                        end += len(storage[key])
                    end += 1
                    items = storage[key][start:end]
                    response = f"*{len(items)}\r\n".encode()
                    for item in items:
                        response += f"${len(item)}\r\n{item}\r\n".encode()
                except ValueError:
                    response = b"-ERR invalid range\r\n"
        elif commands[0] == "LLEN":
            key = commands[1]
            if key not in storage or not isinstance(storage[key], list):
                response = b":0\r\n"
            else:
                response = f":{len(storage[key])}\r\n".encode()
        elif commands[0] == "LPOP":
            key = commands[1]
            if key not in storage or not isinstance(storage[key], list) or not storage[key]:
                response = b"$-1\r\n"
            elif len(commands) == 3:
                try:
                    count = int(commands[2])
                    if count <= 0:
                        response = b"$-1\r\n"
                    else:
                        items_to_pop = min(count, len(storage[key]))
                        values = [storage[key].pop(0) for _ in range(items_to_pop)]
                        response = f"*{len(values)}\r\n".encode()
                        for value in values:
                            response += f"${len(value)}\r\n{value}\r\n".encode()
                except ValueError:
                    response = b"-ERR invalid count\r\n"
            else:
                value = storage[key].pop(0)
                response = f"${len(value)}\r\n{value}\r\n".encode()
        elif commands[0] == "BLPOP":
            key = commands[1]
            timeout = 0
            if len(commands) == 3:
                try:
                    timeout = float(commands[2])
                except ValueError:
                    response = b"-ERR invalid timeout\r\n"
                    return response

            if key in storage and isinstance(storage[key], list) and storage[key]:
                value = storage[key].pop(0)
                response = (
                    f"*2\r\n"
                    f"${len(key)}\r\n{key}\r\n"
                    f"${len(value)}\r\n{value}\r\n"
                ).encode()
            else:
                end_time = datetime.now() + timedelta(seconds=timeout)
                if blocked_clients_by_key.get(key) is None:
                    blocked_clients_by_key[key] = []
                blocked_clients_by_key[key].append(client_socket)
                if timeout > 0:
                    blocked_client_deadline[client_socket] = end_time
                return None
        elif commands[0] == "TYPE":
            key = commands[1]
            if key not in storage:
                response = b"+none\r\n"
            else:
                value = storage[key]
                python_type = type(value).__name__
                if python_type == "str":
                    value_type = "string"
                elif isinstance(value, RedisStream):
                    value_type = "stream"
                else:
                    value_type = python_type
                response = f"+{value_type}\r\n".encode()
        elif commands[0] == "XADD":
            if len(commands) < 5 or len(commands[3:]) % 2 != 0:
                response = b"-ERR wrong number of arguments for 'xadd' command\r\n"
                return response

            key = commands[1]
            entry_id = commands[2]
            fields = commands[3:]

            if key not in storage:
                storage[key] = RedisStream()
            elif not isinstance(storage[key], RedisStream):
                response = b"-ERR wrong type of value for 'xadd' command\r\n"
                return response

            last_id_tuple = (0, 0)
            if storage[key].entries:
                last_id = storage[key].entries[-1][0]
                parsed_last_id = parse_stream_id(last_id)
                if parsed_last_id is not None:
                    last_id_tuple = parsed_last_id

            if entry_id == "*":
                current_milliseconds = int(datetime.now().timestamp() * 1000)
                if current_milliseconds > last_id_tuple[0]:
                    entry_id = f"{current_milliseconds}-0"
                else:
                    entry_id = f"{last_id_tuple[0]}-{last_id_tuple[1] + 1}"
            elif entry_id.count("-") == 1 and entry_id.endswith("-*"):
                milliseconds_part = entry_id.split("-")[0]
                if not milliseconds_part.isdigit():
                    response = b"-ERR Invalid stream ID specified as stream command argument\r\n"
                    return response

                milliseconds_value = int(milliseconds_part)
                if milliseconds_value < last_id_tuple[0]:
                    response = b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
                    return response

                if milliseconds_value == last_id_tuple[0]:
                    sequence_value = last_id_tuple[1] + 1
                elif milliseconds_value == 0:
                    sequence_value = 1
                else:
                    sequence_value = 0

                entry_id = f"{milliseconds_value}-{sequence_value}"
            else:
                parsed_entry_id = parse_stream_id(entry_id)
                if parsed_entry_id is None:
                    response = b"-ERR Invalid stream ID specified as stream command argument\r\n"
                    return response

                if parsed_entry_id == (0, 0):
                    response = b"-ERR The ID specified in XADD must be greater than 0-0\r\n"
                    return response

                if parsed_entry_id <= last_id_tuple:
                    response = b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
                    return response

            storage[key].entries.append((entry_id, fields))
            wake_blocked_xread_clients_for_stream(key)
            response = f"${len(entry_id)}\r\n{entry_id}\r\n".encode()
        elif commands[0] == "XRANGE":
            key = commands[1]
            if key not in storage or not isinstance(storage[key], RedisStream):
                response = b"*0\r\n"
            else:
                start_id = commands[2]
                end_id = commands[3]

                entries = storage[key].entries
                matching_entries = []
                for entry_id, fields in entries:
                    if (start_id == "-" or entry_id >= start_id) and (end_id == "+" or entry_id <= end_id):
                        matching_entries.append((entry_id, fields))

                response = f"*{len(matching_entries)}\r\n".encode()
                for entry_id, fields in matching_entries:
                    response += f"*2\r\n${len(entry_id)}\r\n{entry_id}\r\n".encode()
                    response += f"*{len(fields)}\r\n".encode()
                    for i in range(0, len(fields), 2):
                        field_name = fields[i]
                        field_value = fields[i + 1]
                        response += f"${len(field_name)}\r\n{field_name}\r\n".encode()
                        response += f"${len(field_value)}\r\n{field_value}\r\n".encode()
        elif commands[0] == "XREAD":
            if len(commands) < 4:
                response = b"-ERR wrong number of arguments for 'xread' command\r\n"
                return response

            count = None
            block_milliseconds = None
            index = 1
            while index < len(commands):
                option = commands[index].upper()
                if option == "COUNT":
                    if index + 1 >= len(commands):
                        response = b"-ERR missing COUNT argument for 'xread' command\r\n"
                        return response
                    try:
                        count = int(commands[index + 1])
                    except ValueError:
                        response = b"-ERR invalid COUNT argument for 'xread' command\r\n"
                        return response
                    if count < 0:
                        response = b"-ERR invalid COUNT argument for 'xread' command\r\n"
                        return response
                    index += 2
                elif option == "BLOCK":
                    if index + 1 >= len(commands):
                        response = b"-ERR missing BLOCK argument for 'xread' command\r\n"
                        return response
                    try:
                        block_milliseconds = int(commands[index + 1])
                        if block_milliseconds < 0:
                            response = b"-ERR invalid BLOCK argument for 'xread' command\r\n"
                            return response
                    except ValueError:
                        response = b"-ERR invalid BLOCK argument for 'xread' command\r\n"
                        return response
                    index += 2
                elif option == "STREAMS":
                    index += 1
                    break
                else:
                    response = b"-ERR syntax error\r\n"
                    return response

            if index >= len(commands):
                response = b"-ERR wrong number of arguments for 'xread' command\r\n"
                return response

            stream_keys_and_ids = commands[index:]
            if len(stream_keys_and_ids) % 2 != 0:
                response = b"-ERR invalid stream and ID pairs for 'xread' command\r\n"
                return response

            half = len(stream_keys_and_ids) // 2
            stream_keys = stream_keys_and_ids[:half]
            last_ids = stream_keys_and_ids[half:]
            streams_to_read = list(zip(stream_keys, last_ids))

            should_block = block_milliseconds is not None
            resolved_streams, resolve_error = resolve_xread_stream_offsets(
                streams_to_read,
                resolve_dollar_to_latest=should_block,
            )
            if resolve_error is not None:
                response = resolve_error
                return response

            result_streams = collect_xread_matching_entries(resolved_streams, count)

            if result_streams:
                response = encode_xread_response(result_streams)
                return response

            if should_block:
                deadline = None
                if block_milliseconds > 0:
                    deadline = datetime.now() + timedelta(milliseconds=block_milliseconds)

                blocked_xread_requests.append(
                    BlockedXReadRequest(
                        client_socket=client_socket,
                        resolved_streams=resolved_streams,
                        count=count,
                        deadline=deadline,
                    )
                )
                return None

            response = b"*-1\r\n"
        elif commands[0] == "MULTI":
            response = b"+OK\r\n"
            if client_socket is not None:
                if client_socket in transaction_queue:
                    response = b"-ERR MULTI calls can not be nested\r\n"
                else:
                    transaction_queue[client_socket] = []
        elif commands[0] == "EXEC":
            if client_socket is None:
                response = b"-ERR EXEC without MULTI\r\n"
            elif client_socket not in transaction_queue:
                response = b"-ERR EXEC without MULTI\r\n"
            else:
                queued_commands = transaction_queue[client_socket]
                del transaction_queue[client_socket]

                response = f"*{len(queued_commands)}\r\n".encode()
                for queued_command in queued_commands:
                    queued_response = handle_client(queued_command, client_socket=client_socket)
                    if queued_response is None:
                        queued_response = b"$-1\r\n"
                    response += queued_response

    except Exception as e:
        response = f"-ERR {e}\r\n".encode()

    return response


def close_client_socket(s, inputs, outputs, recv_buffer, send_queue):
    global blocked_xread_requests

    blocked_client_deadline.pop(s, None)
    for clients in blocked_clients_by_key.values():
        if s in clients:
            clients.remove(s)
    blocked_xread_requests = [req for req in blocked_xread_requests if req.client_socket is not s]

    if s in outputs:
        outputs.remove(s)
    if s in inputs:
        inputs.remove(s)
    recv_buffer.pop(s, None)
    send_queue.pop(s, None)
    s.close()

def main():
    global storage
    storage = {}
    global expire_times
    expire_times = {}
    global blocked_clients_by_key
    blocked_clients_by_key = {}
    global blocked_client_deadline
    blocked_client_deadline = {}
    global blocked_xread_requests
    blocked_xread_requests = []
    global commands_queue
    commands_queue = []
    global transaction_queue
    transaction_queue = {}
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    server_socket = socket.create_server(("127.0.0.1", 6379), reuse_port=True)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.setblocking(False)
    server_socket.listen(socket_queue_size)

    inputs = [server_socket]
    global outputs
    outputs = []
    recv_buffer = {}
    global send_queue
    send_queue = {}

    try:
        while True:
            readable, writable, exceptional = select.select(
                inputs, outputs, inputs, socket_timeout)
            for key, deadline in list(blocked_client_deadline.items()):
                if key in send_queue and datetime.now() >= deadline:
                    send_queue[key].append(b"*-1\r\n")
                    if key not in outputs:
                        outputs.append(key)
                    blocked_client_deadline.pop(key, None)
                    for clients in blocked_clients_by_key.values():
                        if key in clients:
                            clients.remove(key)
            for request in list(blocked_xread_requests):
                client = request.client_socket
                if client not in send_queue:
                    blocked_xread_requests.remove(request)
                    continue
                if request.deadline is not None and datetime.now() >= request.deadline:
                    send_queue[client].append(b"*-1\r\n")
                    if client not in outputs:
                        outputs.append(client)
                    blocked_xread_requests.remove(request)
            for s in readable:
                if s is server_socket:
                    client_socket, _ = server_socket.accept()
                    client_socket.setblocking(False)
                    inputs.append(client_socket)
                    recv_buffer[client_socket] = b""
                    send_queue[client_socket] = []
                else:
                    data = s.recv(socket_receive_buffer_size)
                    if data:
                        recv_buffer[s] += data

                        try:
                            while True:
                                commands, remaining = parse_resp_from_buffer(recv_buffer[s])
                                if commands is None:
                                    recv_buffer[s] = remaining
                                    break

                                recv_buffer[s] = remaining
                                command_name = commands[0].upper()
                                if s in transaction_queue and command_name not in ("MULTI", "EXEC"):
                                    if len(transaction_queue[s]) >= transaction_queue_limit:
                                        response = b"-ERR transaction queue is full\r\n"
                                    else:
                                        transaction_queue[s].append(commands)
                                        response = b"+QUEUED\r\n"
                                else:
                                    response = handle_client(commands, client_socket=s)
                                if response is not None:
                                    send_queue[s].append(response)
                                    if s not in outputs:
                                        outputs.append(s)
                        except ValueError as e:
                            send_queue[s].append(f"-ERR {e}\r\n".encode())
                            recv_buffer[s] = b""
                            if s not in outputs:
                                outputs.append(s)
                    else:
                        close_client_socket(s, inputs, outputs, recv_buffer, send_queue)
            for s in writable:
                if s in send_queue and send_queue[s]:
                    try:
                        while send_queue[s]:
                            message = send_queue[s].pop(0)
                            s.sendall(message)

                        if s in outputs:
                            outputs.remove(s)
                    except Exception as e:
                        print(f"处理客户端 {s.getpeername()} 时出错: {e}")
                        close_client_socket(s, inputs, outputs, recv_buffer, send_queue)

            for s in exceptional:
                close_client_socket(s, inputs, outputs, recv_buffer, send_queue)
            
    except Exception as e:
        print(f"服务器发生错误: {e}")


if __name__ == "__main__":
    main()
