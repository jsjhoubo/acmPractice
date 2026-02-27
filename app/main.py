import select
import socket  # noqa: F401
import sys
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


def propagate_to_replicas(commands, source_client=None):
    if not replica_connections:
        return

    encoded_command = encode_resp_array(commands)
    for replica_socket in list(replica_connections):
        if replica_socket is source_client:
            continue
        if replica_socket not in send_queue:
            replica_connections.discard(replica_socket)
            continue
        send_queue[replica_socket].append(encoded_command)
        if replica_socket not in outputs:
            outputs.append(replica_socket)


def handle_client(commands, client_socket=None):
    response = b""
    try:
        if not commands:
            return b"-ERR empty command\r\n"

        if commands[0] == "PING":
            response = b"+PONG\r\n"
        elif commands[0] == "REPLCONF":
            if len(commands) != 3:
                response = b"-ERR wrong number of arguments for 'replconf' command\r\n"
            else:
                if commands[1].upper() == "GETACK" and commands[2] == "*":
                    response = b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
                else:
                    response = b"+OK\r\n"
        elif commands[0] == "PSYNC":
            if len(commands) != 3:
                response = b"-ERR wrong number of arguments for 'psync' command\r\n"
            elif commands[1] != "?" or commands[2] != "-1":
                response = b"-ERR invalid arguments for 'psync' command\r\n"
            else:
                response = f"+FULLRESYNC {master_replid} {master_repl_offset}\r\n".encode()
                if client_socket is not None:
                    replica_connections.add(client_socket)
                empty_rdb_hex = (
                    "524544495330303132fa0972656469732d76657205372e342e38"
                    "fa0a72656469732d62697473c040fa056374696d65c2a111a169"
                    "fa08757365642d6d656dc2d00e0f00fa08616f662d62617365c000ff"
                )
                empty_rdb_bytes = bytes.fromhex(empty_rdb_hex)
                rdb_header = f"${len(empty_rdb_bytes)}\r\n".encode()
                client_socket.sendall(response)
                client_socket.sendall(rdb_header + empty_rdb_bytes)
                response = None
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
                propagate_to_replicas(commands, source_client=client_socket)
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
                propagate_to_replicas(commands, source_client=client_socket)
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
        elif commands[0].upper() == "INFO":
            if len(commands) == 1:
                role_for_info = "slave" if upstream_master_host is not None else "master"
                info_lines = [
                    f"role:{role_for_info}",
                    f"master_replid:{master_replid}",
                    f"master_repl_offset:{master_repl_offset}",
                    f"connected_slaves:{len(replica_connections)}",
                ]
                if upstream_master_host is not None:
                    info_lines.extend(
                        [
                            f"master_host:{upstream_master_host}",
                            f"master_port:{upstream_master_port}",
                            f"master_link_status:{'up' if upstream_connected else 'down'}",
                        ]
                    )
                info_content = "\r\n".join(info_lines)
                response = f"${len(info_content)}\r\n{info_content}\r\n".encode()
            elif len(commands) == 2 and commands[1].lower() == "replication":
                role_for_info = "slave" if upstream_master_host is not None else "master"
                info_lines = [
                    f"role:{role_for_info}",
                    f"master_replid:{master_replid}",
                    f"master_repl_offset:{master_repl_offset}",
                    f"connected_slaves:{len(replica_connections)}",
                ]
                if upstream_master_host is not None:
                    info_lines.extend(
                        [
                            f"master_host:{upstream_master_host}",
                            f"master_port:{upstream_master_port}",
                            f"master_link_status:{'up' if upstream_connected else 'down'}",
                        ]
                    )
                info_content = "\r\n".join(info_lines)
                response = f"${len(info_content)}\r\n{info_content}\r\n".encode()
            else:
                response = b"$0\r\n\r\n"
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
        elif commands[0] == "DISCARD":
            if client_socket is None:
                response = b"-ERR DISCARD without MULTI\r\n"
            elif client_socket not in transaction_queue:
                response = b"-ERR DISCARD without MULTI\r\n"
            else:
                del transaction_queue[client_socket]
                response = b"+OK\r\n"

    except Exception as e:
        response = f"-ERR {e}\r\n".encode()

    return response


def close_client_socket(s, inputs, outputs, recv_buffer, send_queue):
    global blocked_xread_requests

    blocked_client_deadline.pop(s, None)
    replica_connections.discard(s)
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


def encode_resp_array(parts):
    encoded = f"*{len(parts)}\r\n".encode()
    for part in parts:
        encoded_part = part.encode()
        encoded += f"${len(encoded_part)}\r\n".encode() + encoded_part + b"\r\n"
    return encoded


def send_replica_ping(master_host: str, master_port: int, listening_port: int):
    master_socket = socket.create_connection((master_host, master_port), timeout=socket_timeout)
    handshake_buffer = b""

    def read_line_from_master():
        nonlocal handshake_buffer
        while True:
            line_end_index = handshake_buffer.find(b"\r\n")
            if line_end_index != -1:
                line = handshake_buffer[:line_end_index]
                handshake_buffer = handshake_buffer[line_end_index + 2:]
                return line

            chunk = master_socket.recv(socket_receive_buffer_size)
            if not chunk:
                raise ConnectionError("connection closed by master")
            handshake_buffer += chunk

    def read_exact_from_master(expected_size: int):
        nonlocal handshake_buffer
        while len(handshake_buffer) < expected_size:
            chunk = master_socket.recv(socket_receive_buffer_size)
            if not chunk:
                raise ConnectionError("connection closed by master")
            handshake_buffer += chunk

        data = handshake_buffer[:expected_size]
        handshake_buffer = handshake_buffer[expected_size:]
        return data

    try:
        master_socket.sendall(encode_resp_array(["PING"]))
        ping_response = read_line_from_master()
        if ping_response != b"+PONG":
            print(f"Unexpected response from master: {ping_response.decode(errors='replace')}")
            master_socket.close()
            return None, b""

        master_socket.sendall(encode_resp_array(["REPLCONF", "listening-port", str(listening_port)]))
        replconf_port_response = read_line_from_master()
        if replconf_port_response != b"+OK":
            print(f"Unexpected response from master: {replconf_port_response.decode(errors='replace')}")
            master_socket.close()
            return None, b""

        master_socket.sendall(encode_resp_array(["REPLCONF", "capa", "psync2"]))
        replconf_capa_response = read_line_from_master()
        if replconf_capa_response != b"+OK":
            print(f"Unexpected response from master: {replconf_capa_response.decode(errors='replace')}")
            master_socket.close()
            return None, b""

        master_socket.sendall(encode_resp_array(["PSYNC", "?", "-1"]))
        psync_response = read_line_from_master()
        if not psync_response.startswith(b"+FULLRESYNC"):
            print(f"Unexpected response from master: {psync_response.decode(errors='replace')}")
            master_socket.close()
            return None, b""

        rdb_header = read_line_from_master()
        if not rdb_header.startswith(b"$"):
            print(f"Unexpected RDB header from master: {rdb_header.decode(errors='replace')}")
            master_socket.close()
            return None, b""

        rdb_length = int(rdb_header[1:])
        if rdb_length < 0:
            print("Unexpected negative RDB length from master")
            master_socket.close()
            return None, b""

        read_exact_from_master(rdb_length)
    except (ConnectionError, OSError, ValueError) as error:
        print(f"Unexpected response from master: {error}")
        master_socket.close()
        return None, b""

    return master_socket, handshake_buffer

def main():
    port = 6379
    role = "master"
    master_host = None
    master_port = None
    if "--port" in sys.argv:
        port_arg_index = sys.argv.index("--port")
        if port_arg_index + 1 < len(sys.argv):
            port = int(sys.argv[port_arg_index + 1])
    if "--replicaof" in sys.argv:
        replicaof_arg_index = sys.argv.index("--replicaof")
        if replicaof_arg_index + 1 < len(sys.argv):
            role = "slave"
            master_info = sys.argv[replicaof_arg_index + 1]
            if " " in master_info:
                parts = master_info.split()
                if len(parts) != 2:
                    print("Usage: --replicaof \"<master_host> <master_port>\"")
                    return
                master_host, master_port = parts[0], int(parts[1])
            elif ":" in master_info:
                host_part, port_part = master_info.split(":", 1)
                master_host, master_port = host_part, int(port_part)
            else:
                print("Usage: --replicaof \"<master_host> <master_port>\"")
                return
            
        else:
            print("Usage: --replicaof \"<master_host> <master_port>\"")
            return

    global server_role
    server_role = role
    global upstream_master_host
    upstream_master_host = master_host
    global upstream_master_port
    upstream_master_port = master_port
    global upstream_connected
    upstream_connected = False
    global master_replid
    master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    global master_repl_offset
    master_repl_offset = 0

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
    global replica_connections
    replica_connections = set()
    global commands_queue
    commands_queue = []
    global transaction_queue
    transaction_queue = {}
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    server_socket = socket.create_server(("127.0.0.1", port), reuse_port=True)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.setblocking(False)
    server_socket.listen(socket_queue_size)
    inputs = [server_socket]
    master_connection = None
    master_initial_buffer = b""
    if server_role == "slave" and master_host is not None and master_port is not None:
        master_connection, master_initial_buffer = send_replica_ping(master_host, master_port, port)
        if master_connection is None:
            print("Failed to communicate with master, exiting.")
            return
        upstream_connected = True
        master_connection.setblocking(False)
        inputs.append(master_connection)

    
    global outputs
    outputs = []
    recv_buffer = {}
    global send_queue
    send_queue = {}

    def flush_send_queue_for_socket(s):
        if s in send_queue and send_queue[s]:
            while send_queue[s]:
                message = send_queue[s].pop(0)
                s.sendall(message)
        if s in outputs and (s not in send_queue or not send_queue[s]):
            outputs.remove(s)

    if master_connection is not None:
        recv_buffer[master_connection] = master_initial_buffer
        send_queue[master_connection] = []
        if master_initial_buffer:
            try:
                while True:
                    commands, remaining = parse_resp_from_buffer(recv_buffer[master_connection])
                    if commands is None:
                        recv_buffer[master_connection] = remaining
                        break

                    recv_buffer[master_connection] = remaining
                    response = handle_client(commands, client_socket=master_connection)
                    is_getack_request = (
                        len(commands) == 3
                        and commands[0].upper() == "REPLCONF"
                        and commands[1].upper() == "GETACK"
                        and commands[2] == "*"
                    )
                    if not is_getack_request:
                        response = None

                    if response is not None:
                        send_queue[master_connection].append(response)
                        if master_connection not in outputs:
                            outputs.append(master_connection)
                        flush_send_queue_for_socket(master_connection)
            except ValueError:
                recv_buffer[master_connection] = b""

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
                                if s in transaction_queue and command_name not in ("MULTI", "EXEC", "DISCARD"):
                                    if len(transaction_queue[s]) >= transaction_queue_limit:
                                        response = b"-ERR transaction queue is full\r\n"
                                    else:
                                        transaction_queue[s].append(commands)
                                        response = b"+QUEUED\r\n"
                                else:
                                    response = handle_client(commands, client_socket=s)
                                    if master_connection is not None and s is master_connection:
                                        is_getack_request = (
                                            len(commands) == 3
                                            and commands[0].upper() == "REPLCONF"
                                            and commands[1].upper() == "GETACK"
                                            and commands[2] == "*"
                                        )
                                        if not is_getack_request:
                                            response = None
                                if response is not None:
                                    send_queue[s].append(response)
                                    if s not in outputs:
                                        outputs.append(s)
                                    if master_connection is not None and s is master_connection:
                                        flush_send_queue_for_socket(s)
                        except ValueError as e:
                            if master_connection is not None and s is master_connection:
                                recv_buffer[s] = b""
                            else:
                                send_queue[s].append(f"-ERR {e}\r\n".encode())
                                recv_buffer[s] = b""
                                if s not in outputs:
                                    outputs.append(s)
                    else:
                        close_client_socket(s, inputs, outputs, recv_buffer, send_queue)
            for s in writable:
                if s in send_queue and send_queue[s]:
                    try:
                        flush_send_queue_for_socket(s)
                    except Exception as e:
                        print(f"处理客户端 {s.getpeername()} 时出错: {e}")
                        close_client_socket(s, inputs, outputs, recv_buffer, send_queue)

            for s in exceptional:
                close_client_socket(s, inputs, outputs, recv_buffer, send_queue)
            
    except Exception as e:
        print(f"服务器发生错误: {e}")


if __name__ == "__main__":
    main()
