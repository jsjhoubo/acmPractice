import select
import socket  # noqa: F401
import sys
from datetime import datetime, timedelta
import os
import signal
import random
import math

socket_queue_size = 128
socket_receive_buffer_size = 1024
socket_timeout = 1
transaction_queue_limit = 1000
rdb_save_rules_default = [(900, 1), (300, 10), (60, 10000)]
rdb_bgsave_reap_mode_default = "poll"
aof_fsync_interval_seconds = 1
subscribed_mode_allowed_commands = {
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "PING",
    "QUIT",
    "RESET",
}


class RedisStream:
    def __init__(self):
        self.entries = []


class SkipListNode:
    def __init__(self, score: float, member: str, level: int):
        self.score = score
        self.member = member
        self.forward = [None] * level


class RedisSortedSet:
    def __init__(self):
        self.member_scores = {}
        self.max_level = 16
        self.probability = 0.25
        self.level = 1
        self.head = SkipListNode(float("-inf"), "", self.max_level)

    def _random_level(self):
        level = 1
        while level < self.max_level and random.random() < self.probability:
            level += 1
        return level

    def _comes_before(self, node, score: float, member: str):
        return node.score < score or (node.score == score and node.member < member)

    def _find_update_path(self, score: float, member: str):
        update = [None] * self.max_level
        current = self.head
        for level_index in range(self.level - 1, -1, -1):
            while (
                current.forward[level_index] is not None
                and self._comes_before(current.forward[level_index], score, member)
            ):
                current = current.forward[level_index]
            update[level_index] = current
        return update

    def _insert_skiplist_node(self, score: float, member: str):
        update = self._find_update_path(score, member)
        node_level = self._random_level()

        if node_level > self.level:
            for level_index in range(self.level, node_level):
                update[level_index] = self.head
            self.level = node_level

        new_node = SkipListNode(score, member, node_level)
        for level_index in range(node_level):
            new_node.forward[level_index] = update[level_index].forward[level_index]
            update[level_index].forward[level_index] = new_node

    def _remove_skiplist_node(self, score: float, member: str):
        update = self._find_update_path(score, member)
        target = update[0].forward[0]
        if target is None or target.score != score or target.member != member:
            return

        for level_index in range(self.level):
            if update[level_index].forward[level_index] is target:
                update[level_index].forward[level_index] = target.forward[level_index]

        while self.level > 1 and self.head.forward[self.level - 1] is None:
            self.level -= 1

    def add(self, score: float, member: str):
        if member in self.member_scores:
            previous_score = self.member_scores[member]
            if previous_score == score:
                return 0

            self._remove_skiplist_node(previous_score, member)
            self.member_scores[member] = score
            self._insert_skiplist_node(score, member)
            return 0

        self.member_scores[member] = score
        self._insert_skiplist_node(score, member)
        return 1

    def rank(self, member: str):
        if member not in self.member_scores:
            return None

        target_score = self.member_scores[member]
        current = self.head.forward[0]
        rank = 0

        while current is not None and self._comes_before(current, target_score, member):
            rank += 1
            current = current.forward[0]

        if current is not None and current.score == target_score and current.member == member:
            return rank
        return None

    def range(self, start: int, end: int):
        ordered_members = []
        current = self.head.forward[0]
        while current is not None:
            ordered_members.append((current.member, current.score))
            current = current.forward[0]

        member_count = len(ordered_members)
        if member_count == 0:
            return []

        if start < 0:
            start += member_count
        if end < 0:
            end += member_count

        if start < 0:
            start = 0
        if end < 0:
            return []
        if start >= member_count:
            return []
        if end >= member_count:
            end = member_count - 1
        if start > end:
            return []

        return ordered_members[start:end + 1]

    def cardinality(self):
        return len(self.member_scores)

    def score(self, member: str):
        return self.member_scores.get(member)

    def remove(self, member: str):
        if member not in self.member_scores:
            return 0

        score = self.member_scores[member]
        self._remove_skiplist_node(score, member)
        del self.member_scores[member]
        return 1

    def _ordered_members_with_scores(self):
        current = self.head.forward[0]
        while current is not None:
            yield current.member, current.score
            current = current.forward[0]

    def search_by_radius(self, longitude: float, latitude: float, radius_meters: float):
        matching_members = []
        for member, score in self._ordered_members_with_scores():
            member_longitude, member_latitude = decode_geohash(int(score))
            distance = calculate_distance(
                longitude,
                latitude,
                member_longitude,
                member_latitude,
            )
            if distance <= radius_meters:
                matching_members.append(member)
        return matching_members

    def search_by_box(self, longitude: float, latitude: float, width_meters: float, height_meters: float):
        if width_meters < 0 or height_meters < 0:
            return []

        matching_members = []
        half_width = width_meters / 2.0
        half_height = height_meters / 2.0

        meters_per_lat_degree = 111320.0
        meters_per_lon_degree = 111320.0 * max(1e-9, math.cos(math.radians(latitude)))

        for member, score in self._ordered_members_with_scores():
            member_longitude, member_latitude = decode_geohash(int(score))

            lat_distance = abs(member_latitude - latitude) * meters_per_lat_degree
            lon_distance = abs(member_longitude - longitude) * meters_per_lon_degree

            if lat_distance <= half_height and lon_distance <= half_width:
                matching_members.append(member)

        return matching_members


class BlockedXReadRequest:
    def __init__(self, client_socket, resolved_streams, count, deadline):
        self.client_socket = client_socket
        self.resolved_streams = resolved_streams
        self.count = count
        self.deadline = deadline
        self.stream_keys = {stream_key for stream_key, _ in resolved_streams}


def load_rdb_bytes(raw, target_storage, target_expire_times):
    if len(raw) < 9:
        print(f"Loaded RDB bytes: {len(raw)}")
        return

    header = raw[0:9]
    if header[0:5] != b"REDIS":
        print(b"-ERR Invalid RDB file header\r\n")
        return

    pending_expire_at = None
    idx = 9
    while True:
        if idx >= len(raw):
            break

        opcode = raw[idx]
        idx += 1

        if opcode == 0xFA:
            _, idx = read_string(raw, idx)
            _, idx = read_string(raw, idx)
            continue
        if opcode == 0xFE:
            _, idx, _, _ = read_length(raw, idx)
            continue
        if opcode == 0xFB:
            _, idx, _, _ = read_length(raw, idx)
            _, idx, _, _ = read_length(raw, idx)
            continue
        if opcode == 0xFF:
            break
        if opcode == 0xFD:
            if idx + 4 > len(raw):
                raise ValueError("unexpected end of RDB while reading EXPIRETIME")
            expire_seconds = int.from_bytes(raw[idx:idx + 4], byteorder="little", signed=False)
            pending_expire_at = datetime.fromtimestamp(expire_seconds)
            idx += 4
            continue
        if opcode == 0xFC:
            if idx + 8 > len(raw):
                raise ValueError("unexpected end of RDB while reading EXPIRETIMEMS")
            expire_milliseconds = int.from_bytes(raw[idx:idx + 8], byteorder="little", signed=False)
            pending_expire_at = datetime.fromtimestamp(expire_milliseconds / 1000)
            idx += 8
            continue
        if opcode == 0x00:
            key, idx = read_string(raw, idx)
            value, idx = read_string(raw, idx)
            if pending_expire_at is not None and datetime.now() >= pending_expire_at:
                pending_expire_at = None
                continue
            target_storage[key] = value
            if pending_expire_at is not None:
                target_expire_times[key] = pending_expire_at
            pending_expire_at = None
            continue
        raise ValueError(f"unsupported RDB opcode: {opcode}")

def load_rdb_file(dir_path, filename, target_storage):
    file_path = os.path.join(dir_path, filename)
    if not os.path.exists(file_path):
        return b"-ERR RDB file not found\r\n"
    with open(file_path, "rb") as f:
        raw = f.read()
        load_rdb_bytes(raw, target_storage, expire_times)
    return b"+OK\r\n"


def encode_rdb_length(length: int):
    if length < 0:
        raise ValueError("RDB length cannot be negative")
    if length < 64:
        return bytes([length])
    if length < 16384:
        first = 0x40 | ((length >> 8) & 0x3F)
        second = length & 0xFF
        return bytes([first, second])
    return bytes([0x80]) + length.to_bytes(4, byteorder="big", signed=False)


def encode_rdb_string(value: str):
    encoded = value.encode()
    return encode_rdb_length(len(encoded)) + encoded


def build_rdb_snapshot_bytes(source_storage, source_expire_times):
    raw = bytearray(b"REDIS0012")
    now = datetime.now()

    for key, value in source_storage.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue

        expire_at = source_expire_times.get(key)
        if expire_at is not None:
            if now >= expire_at:
                continue
            expire_ms = int(expire_at.timestamp() * 1000)
            raw.append(0xFC)
            raw.extend(expire_ms.to_bytes(8, byteorder="little", signed=False))

        raw.append(0x00)
        raw.extend(encode_rdb_string(key))
        raw.extend(encode_rdb_string(value))

    raw.append(0xFF)
    return bytes(raw)


def persist_rdb_snapshot():
    try:
        raw = build_rdb_snapshot_bytes(storage, expire_times)
        tmp_file_path = os.path.join(redis_config_dir, f"{redis_config_dbfilename}.tmp")
        file_path = os.path.join(redis_config_dir, redis_config_dbfilename)
        with open(tmp_file_path, "wb") as f:
            f.write(raw)
        os.replace(tmp_file_path, file_path)
        return True
    except OSError as error:
        print(f"Failed to persist RDB snapshot: {error}")
        return False


def mark_rdb_snapshot_dirty():
    global rdb_snapshot_dirty
    global rdb_dirty_version
    global rdb_changes_since_last_save
    rdb_snapshot_dirty = True
    rdb_dirty_version += 1
    rdb_changes_since_last_save += 1


def should_trigger_bgsave(now: datetime):
    if not rdb_snapshot_dirty:
        return False

    elapsed_seconds = int((now - last_rdb_save_time).total_seconds())
    for required_seconds, required_changes in rdb_save_rules:
        if elapsed_seconds >= required_seconds and rdb_changes_since_last_save >= required_changes:
            return True
    return False


def handle_sigchld(_signum, _frame):
    global rdb_child_exit_pending
    rdb_child_exit_pending = True


def maybe_start_bgsave(now: datetime):
    global rdb_bgsave_pid
    global rdb_bgsave_version
    global rdb_bgsave_changes_at_start

    if rdb_bgsave_pid is not None:
        return
    if not should_trigger_bgsave(now):
        return

    pid = os.fork()
    if pid == 0:
        success = persist_rdb_snapshot()
        os._exit(0 if success else 1)

    rdb_bgsave_pid = pid
    rdb_bgsave_version = rdb_dirty_version
    rdb_bgsave_changes_at_start = rdb_changes_since_last_save


def poll_bgsave_status(now: datetime):
    global rdb_bgsave_pid
    global rdb_bgsave_version
    global rdb_bgsave_changes_at_start
    global rdb_snapshot_dirty
    global rdb_changes_since_last_save
    global last_rdb_save_time

    if rdb_bgsave_pid is None:
        return

    try:
        finished_pid, status = os.waitpid(rdb_bgsave_pid, os.WNOHANG)
    except ChildProcessError:
        finished_pid = rdb_bgsave_pid
        status = 1

    if finished_pid == 0:
        return

    success = os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
    if success:
        last_rdb_save_time = now
        remaining_changes = max(0, rdb_changes_since_last_save - rdb_bgsave_changes_at_start)
        rdb_changes_since_last_save = remaining_changes
        rdb_snapshot_dirty = remaining_changes > 0

    rdb_bgsave_pid = None
    rdb_bgsave_version = 0
    rdb_bgsave_changes_at_start = 0


def maybe_reap_bgsave_status(now: datetime):
    global rdb_child_exit_pending

    if rdb_bgsave_reap_mode == "sigchld":
        if not rdb_child_exit_pending:
            return
        rdb_child_exit_pending = False

    poll_bgsave_status(now)


def append_to_aof(commands):
    global next_aof_fsync_at

    if not aof_enabled or aof_file is None:
        return

    encoded_command = encode_resp_array(commands)

    if aof_fsync_policy == "always":
        aof_file.write(encoded_command)
        aof_file.flush()
        os.fsync(aof_file.fileno())
        return

    aof_pending_chunks.append(encoded_command)


def flush_aof_if_needed(now: datetime):
    global next_aof_fsync_at

    if not aof_enabled or aof_file is None:
        return

    if aof_pending_chunks:
        aof_file.write(b"".join(aof_pending_chunks))
        aof_pending_chunks.clear()
        aof_file.flush()

    if aof_fsync_policy == "everysec" and now >= next_aof_fsync_at:
        os.fsync(aof_file.fileno())
        next_aof_fsync_at = now + timedelta(seconds=aof_fsync_interval_seconds)


def read_length(raw: bytes, idx: int):
    if idx >= len(raw):
        raise ValueError("unexpected end of RDB while reading length")

    first_byte = raw[idx]
    idx += 1
    length_type = (first_byte & 0xC0) >> 6

    if length_type == 0:
        return first_byte & 0x3F, idx, False, None

    if length_type == 1:
        if idx >= len(raw):
            raise ValueError("unexpected end of RDB while reading 14-bit length")
        length = ((first_byte & 0x3F) << 8) | raw[idx]
        idx += 1
        return length, idx, False, None

    if length_type == 2:
        if idx + 4 > len(raw):
            raise ValueError("unexpected end of RDB while reading 32-bit length")
        length = int.from_bytes(raw[idx:idx + 4], byteorder="big", signed=False)
        idx += 4
        return length, idx, False, None

    encoding_type = first_byte & 0x3F
    return None, idx, True, encoding_type


def read_string(raw: bytes, idx: int):
    length, idx, is_encoded, encoding_type = read_length(raw, idx)

    if not is_encoded:
        if idx + length > len(raw):
            raise ValueError("unexpected end of RDB while reading string")
        value = raw[idx:idx + length].decode()
        idx += length
        return value, idx

    if encoding_type == 0:
        if idx + 1 > len(raw):
            raise ValueError("unexpected end of RDB while reading int8")
        value = int.from_bytes(raw[idx:idx + 1], byteorder="little", signed=True)
        idx += 1
        return str(value), idx

    if encoding_type == 1:
        if idx + 2 > len(raw):
            raise ValueError("unexpected end of RDB while reading int16")
        value = int.from_bytes(raw[idx:idx + 2], byteorder="little", signed=True)
        idx += 2
        return str(value), idx

    if encoding_type == 2:
        if idx + 4 > len(raw):
            raise ValueError("unexpected end of RDB while reading int32")
        value = int.from_bytes(raw[idx:idx + 4], byteorder="little", signed=True)
        idx += 4
        return str(value), idx

    raise ValueError(f"unsupported encoded string format: {encoding_type}")

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

GEO_MIN_LATITUDE = -85.05112878
GEO_MAX_LATITUDE = 85.05112878
GEO_MIN_LONGITUDE = -180.0
GEO_MAX_LONGITUDE = 180.0
GEO_LATITUDE_RANGE = GEO_MAX_LATITUDE - GEO_MIN_LATITUDE
GEO_LONGITUDE_RANGE = GEO_MAX_LONGITUDE - GEO_MIN_LONGITUDE
GEO_EARTH_RADIUS_METERS = 6372797.560856


def spread_int32_to_int64(value: int):
    result = value & 0xFFFFFFFF
    result = (result | (result << 16)) & 0x0000FFFF0000FFFF
    result = (result | (result << 8)) & 0x00FF00FF00FF00FF
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F
    result = (result | (result << 2)) & 0x3333333333333333
    result = (result | (result << 1)) & 0x5555555555555555
    return result


def compact_int64_to_int32(value: int):
    result = value & 0x5555555555555555
    result = (result | (result >> 1)) & 0x3333333333333333
    result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F
    result = (result | (result >> 4)) & 0x00FF00FF00FF00FF
    result = (result | (result >> 8)) & 0x0000FFFF0000FFFF
    result = (result | (result >> 16)) & 0x00000000FFFFFFFF
    return int(result)


def encode_geohash(longitude: float, latitude: float):
    if not (GEO_MIN_LONGITUDE <= longitude <= GEO_MAX_LONGITUDE):
        raise ValueError("longitude out of range [-180, 180]")
    if not (GEO_MIN_LATITUDE <= latitude <= GEO_MAX_LATITUDE):
        raise ValueError("latitude out of range [-85.05112878, 85.05112878]")

    normalized_latitude = (2**26) * (latitude - GEO_MIN_LATITUDE) / GEO_LATITUDE_RANGE
    normalized_longitude = (2**26) * (longitude - GEO_MIN_LONGITUDE) / GEO_LONGITUDE_RANGE

    lat_int = int(normalized_latitude)
    lon_int = int(normalized_longitude)

    spread_lat = spread_int32_to_int64(lat_int)
    spread_lon = spread_int32_to_int64(lon_int)
    return spread_lat | (spread_lon << 1)


def decode_geohash(score: int):
    grid_latitude_number = compact_int64_to_int32(score)
    grid_longitude_number = compact_int64_to_int32(score >> 1)

    divisor = float(2**26)

    grid_latitude_min = GEO_MIN_LATITUDE + GEO_LATITUDE_RANGE * (grid_latitude_number / divisor)
    grid_latitude_max = GEO_MIN_LATITUDE + GEO_LATITUDE_RANGE * ((grid_latitude_number + 1) / divisor)
    grid_longitude_min = GEO_MIN_LONGITUDE + GEO_LONGITUDE_RANGE * (grid_longitude_number / divisor)
    grid_longitude_max = GEO_MIN_LONGITUDE + GEO_LONGITUDE_RANGE * ((grid_longitude_number + 1) / divisor)

    latitude = (grid_latitude_min + grid_latitude_max) / 2.0
    longitude = (grid_longitude_min + grid_longitude_max) / 2.0
    return longitude, latitude


def calculate_distance(longitude1: float, latitude1: float, longitude2: float, latitude2: float):
    latitude1_rad = math.radians(latitude1)
    latitude2_rad = math.radians(latitude2)
    delta_latitude_rad = latitude2_rad - latitude1_rad
    delta_longitude_rad = math.radians(longitude2 - longitude1)

    haversine_value = (
        math.sin(delta_latitude_rad / 2.0) ** 2
        + math.cos(latitude1_rad)
        * math.cos(latitude2_rad)
        * (math.sin(delta_longitude_rad / 2.0) ** 2)
    )
    central_angle = 2.0 * math.asin(math.sqrt(haversine_value))
    return GEO_EARTH_RADIUS_METERS * central_angle

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
    global master_repl_offset
    if not replica_connections:
        return

    encoded_command = encode_resp_array(commands)

    master_repl_offset += len(encoded_command)
    if source_client is not None:
        client_last_write_offset[source_client] = master_repl_offset
    for replica_socket in list(replica_connections):
        if replica_socket is source_client:
            continue
        if replica_socket not in send_queue:
            replica_connections.discard(replica_socket)
            continue
        send_queue[replica_socket].append(encoded_command)
        if replica_socket not in outputs:
            outputs.append(replica_socket)


def get_subscription_count(client_socket):
    if client_socket is None:
        return 0
    return sum(1 for clients in subscriptions.values() if client_socket in clients)


def remove_client_from_all_subscriptions(client_socket):
    if client_socket is None:
        return

    for channel in list(subscriptions.keys()):
        clients = subscriptions[channel]
        if client_socket in clients:
            clients.remove(client_socket)
        if not clients:
            del subscriptions[channel]


def encode_subscription_error(command_name: str):
    return (
        f"-ERR Can't execute '{command_name.lower()}' in subscribed mode\r\n"
    ).encode()


def promote_to_master(reason: str):
    global server_role
    global upstream_master_host
    global upstream_master_port
    global upstream_connected
    global last_failover_reason
    global last_failover_at
    global failover_term
    global parent_node_id
    global election_voted_term
    global election_voted_for

    if upstream_master_host is None:
        return False

    server_role = "master"
    upstream_master_host = None
    upstream_master_port = None
    upstream_connected = False
    failover_term += 1
    election_voted_term = 0
    election_voted_for = None
    parent_node_id = None
    last_failover_reason = reason
    last_failover_at = datetime.now()
    print(f"Auto failover promoted replica to master: {reason}")
    return True


def parse_peer_endpoint(peer: str):
    if ":" not in peer:
        return None
    host, port_text = peer.split(":", 1)
    if not host:
        return None
    try:
        parsed_port = int(port_text)
    except ValueError:
        return None
    if parsed_port <= 0:
        return None
    return host, parsed_port


def request_vote_from_peer(peer_host: str, peer_port: int, term: int, candidate_id: str):
    vote_socket = None
    try:
        vote_socket = socket.create_connection((peer_host, peer_port), timeout=0.5)
        vote_socket.sendall(
            encode_resp_array(["FAILOVER", "VOTE", str(term), candidate_id])
        )

        response = b""
        while b"\r\n" not in response:
            chunk = vote_socket.recv(socket_receive_buffer_size)
            if not chunk:
                break
            response += chunk
        return response.startswith(b"+YES\r\n")
    except OSError:
        return False
    finally:
        if vote_socket is not None:
            vote_socket.close()


def attempt_failover_election(reason: str):
    global failover_term
    global election_voted_term
    global election_voted_for
    global last_election_term
    global last_election_votes
    global last_failover_reason
    global last_failover_at

    if upstream_master_host is None:
        return False

    election_term = failover_term + 1
    failover_term = election_term
    election_voted_term = election_term
    election_voted_for = node_id

    votes = 1
    for peer_host, peer_port in failover_peer_nodes:
        if request_vote_from_peer(peer_host, peer_port, election_term, node_id):
            votes += 1

    last_election_term = election_term
    last_election_votes = votes

    if votes >= failover_quorum:
        failover_term = election_term - 1
        return promote_to_master(
            f"{reason}; election term={election_term} votes={votes}/{failover_quorum}"
        )

    last_failover_reason = (
        f"election failed: term={election_term} votes={votes}/{failover_quorum}"
    )
    last_failover_at = datetime.now()
    return False


def get_replication_role():
    # Role relative to the upstream link.
    return "replica" if upstream_master_host is not None else "primary"


def get_serving_role():
    # Role relative to downstream replicas.
    has_downstream = len(replica_connections) > 0
    if get_replication_role() == "replica":
        return "relay" if has_downstream else "leaf"
    return "source" if has_downstream else "standalone"


def format_failover_info():
    replication_role = get_replication_role()
    serving_role = get_serving_role()
    legacy_role = "slave" if replication_role == "replica" else "master"
    mode_text = "enabled" if auto_failover_enabled else "disabled"
    timeout_text = str(failover_timeout_ms)
    reason_text = last_failover_reason if last_failover_reason else "none"
    at_text = last_failover_at.isoformat() if last_failover_at is not None else "never"
    return {
        "node_id": node_id,
        "term": str(failover_term),
        "parent_node_id": parent_node_id if parent_node_id else "none",
        "quorum": str(failover_quorum),
        "known_peers": str(len(failover_peer_nodes)),
        "last_election_term": str(last_election_term),
        "last_election_votes": str(last_election_votes),
        "role": legacy_role,
        "replication_role": replication_role,
        "serving_role": serving_role,
        "mode": mode_text,
        "timeout_ms": timeout_text,
        "last_reason": reason_text,
        "last_at": at_text,
    }


def request_replicaof_change(host: str | None, port: int | None):
    global pending_replicaof_target
    pending_replicaof_target = (host, port)


def maybe_grant_vote(requested_term: int, candidate_id: str):
    global failover_term
    global election_voted_term
    global election_voted_for

    if requested_term < failover_term:
        return False, "stale term"

    if requested_term > failover_term:
        failover_term = requested_term
        election_voted_term = 0
        election_voted_for = None

    if (
        election_voted_term == requested_term
        and election_voted_for is not None
        and election_voted_for != candidate_id
    ):
        return False, "already voted"

    election_voted_term = requested_term
    election_voted_for = candidate_id
    return True, "granted"


def handle_client(commands, client_socket=None):
    response = b""
    try:
        if not commands:
            return b"-ERR empty command\r\n"

        command_name = commands[0].upper()
        if (
            get_subscription_count(client_socket) > 0
            and command_name not in subscribed_mode_allowed_commands
        ):
            return encode_subscription_error(command_name)

        if command_name == "PING":
            if get_subscription_count(client_socket) > 0:
                response = b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"
            else:
                response = b"+PONG\r\n"
        elif command_name == "PUBLISH":
            if len(commands) != 3:
                response = b"-ERR wrong number of arguments for 'publish' command\r\n"
            else:
                channel = commands[1]
                message = commands[2]
                propagate_to_replicas(commands, source_client=client_socket)
                receivers = 0
                if channel in subscriptions:
                    for client in subscriptions[channel]:
                        if client not in send_queue:
                            continue
                        send_queue[client].append(
                            f"*3\r\n$7\r\nmessage\r\n${len(channel)}\r\n{channel}\r\n${len(message)}\r\n{message}\r\n".encode()
                        )
                        if client not in outputs:
                            outputs.append(client)
                        receivers += 1
                response = f":{receivers}\r\n".encode()
        elif command_name == "SUBSCRIBE":
            if len(commands) < 2:
                response = b"-ERR wrong number of arguments for 'subscribe' command\r\n"
            else:
                for channel in commands[1:]:
                    if channel not in subscriptions:
                        subscriptions[channel] = set()
                    subscriptions[channel].add(client_socket)
                number_subscriptions = len([1 for k, v in subscriptions.items() if client_socket in v])
                response = b"".join(
                    f"*3\r\n$9\r\nsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{number_subscriptions}\r\n".encode()
                    for channel in commands[1:]
                )
        elif command_name == "UNSUBSCRIBE":
            if client_socket is None:
                response = b"-ERR invalid client\r\n"
            else:
                channels_to_remove = commands[1:]
                if not channels_to_remove:
                    channels_to_remove = [
                        channel for channel, clients in subscriptions.items() if client_socket in clients
                    ]

                payload = b""
                for channel in channels_to_remove:
                    if channel in subscriptions and client_socket in subscriptions[channel]:
                        subscriptions[channel].remove(client_socket)
                        if not subscriptions[channel]:
                            del subscriptions[channel]
                    remaining = get_subscription_count(client_socket)
                    payload += (
                        f"*3\r\n$11\r\nunsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{remaining}\r\n".encode()
                    )

                if payload:
                    response = payload
                else:
                    response = b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
        elif command_name == "RESET":
            remove_client_from_all_subscriptions(client_socket)
            response = b"+RESET\r\n"
        elif command_name == "KEYS":
            if len(commands) != 2:
                response = b"-ERR wrong number of arguments for 'keys' command\r\n"
            else:
                pattern = commands[1]
                if pattern == "*":
                    matching_keys = list(storage.keys())
                else:
                    matching_keys = [key for key in storage.keys() if key == pattern]
                response = f"*{len(matching_keys)}\r\n".encode()
                for key in matching_keys:
                    response += f"${len(key)}\r\n{key}\r\n".encode()
        elif command_name == "CONFIG":
            if len(commands) != 3 or commands[1].upper() != "GET":
                response = b"-ERR wrong number of arguments for 'config' command\r\n"
            else:
                config_key = commands[2].lower()
                if config_key == "dir":
                    response = encode_resp_array(["dir", redis_config_dir])
                elif config_key == "dbfilename":
                    response = encode_resp_array(["dbfilename", redis_config_dbfilename])
                else:
                    response = b"*0\r\n"
        elif command_name == "REPLICAOF":
            if len(commands) != 3:
                response = b"-ERR wrong number of arguments for 'replicaof' command\r\n"
            elif commands[1].upper() == "NO" and commands[2].upper() == "ONE":
                request_replicaof_change(None, None)
                response = b"+OK\r\n"
            else:
                try:
                    target_port = int(commands[2])
                except ValueError:
                    response = b"-ERR invalid master port for 'replicaof' command\r\n"
                else:
                    request_replicaof_change(commands[1], target_port)
                    response = b"+OK\r\n"
        elif command_name == "REPLCONF":
            if len(commands) != 3:
                response = b"-ERR wrong number of arguments for 'replconf' command\r\n"
            else:
                if commands[1].upper() == "GETACK" and commands[2] == "*":
                    response = b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
                elif commands[1].upper() == "ACK":
                    try:
                        ack_offset = int(commands[2])
                    except ValueError:
                        response = b"-ERR invalid replconf ack offset\r\n"
                    else:
                        if client_socket is not None and client_socket in replica_connections:
                            replica_ack_offsets[client_socket] = ack_offset
                        response = None
                else:
                    response = b"+OK\r\n"
        elif command_name == "WAIT":
            if len(commands) != 3:
                response = b"-ERR wrong number of arguments for 'wait' command\r\n"
            else:
                try:
                    num_replicas = int(commands[1])
                    timeout_ms = int(commands[2])
                    if num_replicas < 0 or timeout_ms < 0:
                        response = b"-ERR invalid arguments for 'wait' command\r\n"
                    else:
                        replicas_acked = min(num_replicas, len(replica_connections))
                        target_offset  = client_last_write_offset.get(client_socket, master_repl_offset)
                        replicas_acked = sum(
                            1 for replica_socket in replica_connections
                            if replica_ack_offsets.get(replica_socket, 0) >= target_offset
                        )
                        if replicas_acked >= num_replicas:
                            response = f":{replicas_acked}\r\n".encode()
                        else:
                            pending_wait_requests[client_socket] = {
                                "target_replicas": num_replicas,
                                "target_offset": target_offset,
                                "deadline": datetime.now() + timedelta(milliseconds=timeout_ms),
                            }
                            getack = encode_resp_array(["REPLCONF", "GETACK", "*"])
                            for replica_socket in list(replica_connections):
                                if replica_socket not in send_queue:
                                    continue
                                send_queue[replica_socket].append(getack)
                                if replica_socket not in outputs:
                                    outputs.append(replica_socket)
                            response = None
                except ValueError:
                    response = b"-ERR invalid arguments for 'wait' command\r\n"
        elif command_name == "PSYNC":
            if len(commands) != 3:
                response = b"-ERR wrong number of arguments for 'psync' command\r\n"
            elif commands[1] != "?" or commands[2] != "-1":
                response = b"-ERR invalid arguments for 'psync' command\r\n"
            else:
                response = f"+FULLRESYNC {master_replid} {master_repl_offset}\r\n".encode()
                if client_socket is not None:
                    replica_connections.add(client_socket)
                    replica_ack_offsets[client_socket] = 0
                snapshot_rdb_bytes = build_rdb_snapshot_bytes(storage, expire_times)
                rdb_header = f"${len(snapshot_rdb_bytes)}\r\n".encode()
                client_socket.sendall(response)
                client_socket.sendall(rdb_header + snapshot_rdb_bytes)
                response = None
        elif command_name == "ECHO":
            if len(commands) != 2:
                response = b"-ERR wrong number of arguments for 'echo' command\r\n"
            else:
                response = f"${len(commands[1])}\r\n{commands[1]}\r\n".encode()
        elif command_name == "SET":
            if len(commands) == 3:
                key = commands[1]
                value = commands[2]
                storage[key] = value
                expire_times.pop(key, None)
                propagate_to_replicas(commands, source_client=client_socket)
                append_to_aof(commands)
                mark_rdb_snapshot_dirty()
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
                append_to_aof(commands)
                mark_rdb_snapshot_dirty()
                response = b"+OK\r\n"
            else:
                response = b"-ERR wrong number of arguments for 'set' command\r\n"
        elif command_name == "GET":
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
        elif command_name == "INCR":
            if len(commands) != 2:
                response = b"-ERR wrong number of arguments for 'incr' command\r\n"
            else:
                key = commands[1]
                value = storage.get(key)

                if value is None:
                    value = "1"
                    storage[key] = value
                    propagate_to_replicas(commands, source_client=client_socket)
                    append_to_aof(commands)
                    mark_rdb_snapshot_dirty()
                    response = b":1\r\n"
                else:
                    try:
                        incremented_value = int(value) + 1
                    except ValueError:
                        response = b"-ERR value is not an integer or out of range\r\n"
                    else:
                        storage[key] = str(incremented_value)
                        propagate_to_replicas(commands, source_client=client_socket)
                        append_to_aof(commands)
                        mark_rdb_snapshot_dirty()
                        response = f":{incremented_value}\r\n".encode()
        elif command_name == "RPUSH":
            key = commands[1]
            if key not in storage:
                storage[key] = []
            elif not isinstance(storage[key], list):
                response = b"-ERR wrong type of value for 'rpush' command\r\n"
                return response
            for value in commands[2:]:
                storage[key].append(value)
            propagate_to_replicas(commands, source_client=client_socket)
            append_to_aof(commands)
            pushed_length = len(storage[key])
            wake_blocked_clients_for_key(key, len(commands) - 2)
            response = f":{pushed_length}\r\n".encode()
        elif command_name == "LPUSH":
            key = commands[1]
            if key not in storage:
                storage[key] = []
            elif not isinstance(storage[key], list):
                response = b"-ERR wrong type of value for 'lpush' command\r\n"
                return response
            for value in commands[2:]:
                storage[key].insert(0, value)
            propagate_to_replicas(commands, source_client=client_socket)
            append_to_aof(commands)
            pushed_length = len(storage[key])
            wake_blocked_clients_for_key(key, len(commands) - 2)
            response = f":{pushed_length}\r\n".encode()
        elif command_name == "LRANGE":
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
        elif command_name == "LLEN":
            key = commands[1]
            if key not in storage or not isinstance(storage[key], list):
                response = b":0\r\n"
            else:
                response = f":{len(storage[key])}\r\n".encode()
        elif command_name == "LPOP":
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
                        propagate_to_replicas(commands, source_client=client_socket)
                        append_to_aof(commands)
                        response = f"*{len(values)}\r\n".encode()
                        for value in values:
                            response += f"${len(value)}\r\n{value}\r\n".encode()
                except ValueError:
                    response = b"-ERR invalid count\r\n"
            else:
                value = storage[key].pop(0)
                propagate_to_replicas(commands, source_client=client_socket)
                append_to_aof(commands)
                response = f"${len(value)}\r\n{value}\r\n".encode()
        elif command_name == "BLPOP":
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
        elif command_name == "TYPE":
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
                elif isinstance(value, RedisSortedSet):
                    value_type = "zset"
                else:
                    value_type = python_type
                response = f"+{value_type}\r\n".encode()
        elif command_name == "ZADD":
            if len(commands) < 4 or (len(commands) - 2) % 2 != 0:
                response = b"-ERR wrong number of arguments for 'zadd' command\r\n"
            else:
                key = commands[1]
                if key not in storage:
                    storage[key] = RedisSortedSet()
                elif not isinstance(storage[key], RedisSortedSet):
                    response = b"-ERR wrong type of value for 'zadd' command\r\n"
                    return response

                added_members = 0
                index = 2
                while index < len(commands):
                    score_raw = commands[index]
                    member = commands[index + 1]
                    try:
                        score = float(score_raw)
                    except ValueError:
                        response = b"-ERR value is not a valid float\r\n"
                        return response

                    added_members += storage[key].add(score, member)
                    index += 2

                propagate_to_replicas(commands, source_client=client_socket)
                append_to_aof(commands)
                response = f":{added_members}\r\n".encode()
        elif command_name == "ZREM":
            if len(commands) < 3:
                response = b"-ERR wrong number of arguments for 'zrem' command\r\n"
            else:
                key = commands[1]
                if key not in storage:
                    response = b":0\r\n"
                elif not isinstance(storage[key], RedisSortedSet):
                    response = b"-ERR wrong type of value for 'zrem' command\r\n"
                else:
                    removed_members = 0
                    for member in commands[2:]:
                        removed_members += storage[key].remove(member)

                    if removed_members > 0:
                        propagate_to_replicas(commands, source_client=client_socket)
                        append_to_aof(commands)
                    response = f":{removed_members}\r\n".encode()
        elif command_name == "ZRANK":
            if len(commands) != 3:
                response = b"-ERR wrong number of arguments for 'zrank' command\r\n"
                return response
            key = commands[1]
            member = commands[2]
            if key not in storage or not isinstance(storage[key], RedisSortedSet):
                response = b"$-1\r\n"
            else:
                rank = storage[key].rank(member)
                if rank is None:
                    response = b"$-1\r\n"
                else:
                    response = f":{rank}\r\n".encode()
        elif command_name == "ZRANGE":
            if len(commands) not in {4, 5}:
                response = b"-ERR wrong number of arguments for 'zrange' command\r\n"
                return response

            key = commands[1]
            try:
                start = int(commands[2])
                end = int(commands[3])
            except ValueError:
                response = b"-ERR value is not an integer or out of range\r\n"
                return response

            withscores = len(commands) == 5
            if withscores and commands[4].upper() != "WITHSCORES":
                response = b"-ERR syntax error\r\n"
                return response

            if key not in storage or not isinstance(storage[key], RedisSortedSet):
                response = b"*0\r\n"
            else:
                members = storage[key].range(start, end)
                element_count = len(members) * 2 if withscores else len(members)
                response = f"*{element_count}\r\n".encode()
                for member, score in members:
                    response += f"${len(member)}\r\n{member}\r\n".encode()
                    if withscores:
                        score_str = str(score)
                        response += f"${len(score_str)}\r\n{score_str}\r\n".encode()
        elif command_name == "ZCARD":
            if len(commands) != 2:
                response = b"-ERR wrong number of arguments for 'zcard' command\r\n"
                return response
            key = commands[1]
            if key not in storage or not isinstance(storage[key], RedisSortedSet):
                response = b":0\r\n"
            else:
                response = f":{storage[key].cardinality()}\r\n".encode()
        elif command_name == "ZSCORE":
            if len(commands) != 3:
                response = b"-ERR wrong number of arguments for 'zscore' command\r\n"
                return response
            key = commands[1]
            member = commands[2]
            if key not in storage or not isinstance(storage[key], RedisSortedSet):
                response = b"$-1\r\n"
            else:
                score = storage[key].score(member)
                if score is None:
                    response = b"$-1\r\n"
                else:
                    score_str = str(score)
                    response = f"${len(score_str)}\r\n{score_str}\r\n".encode()
        elif command_name == "GEOADD":
            if len(commands) < 5 or (len(commands) - 2) % 3 != 0:
                response = b"-ERR wrong number of arguments for 'geoadd' command\r\n"
            else:
                key = commands[1]
                if key not in storage:
                    storage[key] = RedisSortedSet()
                elif not isinstance(storage[key], RedisSortedSet):
                    response = b"-ERR wrong type of value for 'geoadd' command\r\n"
                    return response

                added_members = 0
                index = 2
                while index < len(commands):
                    try:
                        longitude = float(commands[index])
                        latitude = float(commands[index + 1])
                    except ValueError:
                        response = b"-ERR invalid longitude or latitude value\r\n"
                        return response

                    member = commands[index + 2]
                    score = encode_geohash(longitude, latitude)
                    added_members += storage[key].add(score, member)
                    index += 3

                propagate_to_replicas(commands, source_client=client_socket)
                append_to_aof(commands)
                response = f":{added_members}\r\n".encode()
        elif command_name == "GEOPOS":
            if len(commands) < 3:
                response = b"-ERR wrong number of arguments for 'geopos' command\r\n"
            else:
                key = commands[1]
                members = commands[2:]
                response = f"*{len(members)}\r\n".encode()

                if key not in storage or not isinstance(storage[key], RedisSortedSet):
                    response += b"*-1\r\n" * len(members)
                else:
                    for member in members:
                        score = storage[key].score(member)
                        if score is None:
                            response += b"*-1\r\n"
                            continue

                        longitude, latitude = decode_geohash(int(score))
                        longitude_str = str(longitude)
                        latitude_str = str(latitude)
                        response += (
                            f"*2\r\n"
                            f"${len(longitude_str)}\r\n{longitude_str}\r\n"
                            f"${len(latitude_str)}\r\n{latitude_str}\r\n"
                        ).encode()
        elif command_name == "GEODIST":
            if len(commands) != 4:
                response = b"-ERR wrong number of arguments for 'geodist' command\r\n"
            else:
                key = commands[1]
                member1 = commands[2]
                member2 = commands[3]
                if key not in storage or not isinstance(storage[key], RedisSortedSet):
                    response = b"$-1\r\n"
                else:
                    score1 = storage[key].score(member1)
                    score2 = storage[key].score(member2)
                    if score1 is None or score2 is None:
                        response = b"$-1\r\n"
                    else:
                        longitude1, latitude1 = decode_geohash(int(score1))
                        longitude2, latitude2 = decode_geohash(int(score2))
                        distance_meters = calculate_distance(longitude1, latitude1, longitude2, latitude2)
                        
                        distance = distance_meters
                        distance_str = str(distance)
                        response = f"${len(distance_str)}\r\n{distance_str}\r\n".encode()
        elif command_name == "GEOSEARCH":
            if len(commands) < 4:
                response = b"-ERR wrong number of arguments for 'geosearch' command\r\n"
            else:
                key = commands[1]
                if key not in storage or not isinstance(storage[key], RedisSortedSet):
                    response = b"*0\r\n"
                else:
                    query_type = commands[2].upper()
                    if query_type == "BYRADIUS":
                        try:
                            longitude = float(commands[3])
                            latitude = float(commands[4])
                            radius = float(commands[5])
                        except ValueError:
                            response = b"-ERR invalid longitude, latitude, or radius value\r\n"
                            return response
                        matching_members = storage[key].search_by_radius(longitude, latitude, radius)
                    elif query_type == "BYBOX":
                        try:
                            longitude = float(commands[3])
                            latitude = float(commands[4])
                            width = float(commands[5])
                            height = float(commands[6])
                        except ValueError:
                            response = b"-ERR invalid longitude, latitude, width, or height value\r\n"
                            return response
                        matching_members = storage[key].search_by_box(longitude, latitude, width, height)
                    else:
                        response = b"-ERR syntax error\r\n"
                        return response

                    response = f"*{len(matching_members)}\r\n".encode()
                    for member in matching_members:
                        response += f"${len(member)}\r\n{member}\r\n".encode()
        elif command_name == "INFO":
            if len(commands) == 1:
                role_for_info = "slave" if get_replication_role() == "replica" else "master"
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
                role_for_info = "slave" if get_replication_role() == "replica" else "master"
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
        elif command_name == "FAILOVER":
            if len(commands) < 2:
                response = b"-ERR wrong number of arguments for 'failover' command\r\n"
            else:
                subcommand = commands[1].upper()
                if subcommand == "STATUS" and len(commands) == 2:
                    failover_info = format_failover_info()
                    payload = "\r\n".join(
                        [
                            f"node_id:{failover_info['node_id']}",
                            f"failover_term:{failover_info['term']}",
                            f"parent_node_id:{failover_info['parent_node_id']}",
                            f"failover_quorum:{failover_info['quorum']}",
                            f"failover_known_peers:{failover_info['known_peers']}",
                            f"failover_last_election_term:{failover_info['last_election_term']}",
                            f"failover_last_election_votes:{failover_info['last_election_votes']}",
                            f"role:{failover_info['role']}",
                            f"replication_role:{failover_info['replication_role']}",
                            f"serving_role:{failover_info['serving_role']}",
                            f"failover_mode:{failover_info['mode']}",
                            f"failover_timeout_ms:{failover_info['timeout_ms']}",
                            f"failover_last_reason:{failover_info['last_reason']}",
                            f"failover_last_at:{failover_info['last_at']}",
                        ]
                    )
                    response = f"${len(payload)}\r\n{payload}\r\n".encode()
                elif subcommand == "FORCE" and len(commands) == 2:
                    promoted = promote_to_master("manual FORCE")
                    if promoted:
                        response = b"+OK\r\n"
                    else:
                        response = b"-ERR FAILOVER FORCE only valid on replica\r\n"
                elif subcommand == "ELECT" and len(commands) == 2:
                    promoted = attempt_failover_election("manual ELECT")
                    if promoted:
                        response = b"+OK\r\n"
                    else:
                        response = b"-ERR election did not reach quorum\r\n"
                elif subcommand == "VOTE" and len(commands) == 4:
                    try:
                        requested_term = int(commands[2])
                    except ValueError:
                        response = b"-ERR invalid election term\r\n"
                    else:
                        candidate_id = commands[3]
                        granted, vote_reason = maybe_grant_vote(requested_term, candidate_id)
                        if granted:
                            response = b"+YES\r\n"
                        else:
                            response = f"+NO {vote_reason}\r\n".encode()
                else:
                    response = b"-ERR syntax error\r\n"
        elif command_name == "XADD":
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
            propagate_to_replicas(commands, source_client=client_socket)
            append_to_aof(commands)
            wake_blocked_xread_clients_for_stream(key)
            response = f"${len(entry_id)}\r\n{entry_id}\r\n".encode()
        elif command_name == "XRANGE":
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
        elif command_name == "XREAD":
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
        elif command_name == "MULTI":
            response = b"+OK\r\n"
            if client_socket is not None:
                if client_socket in transaction_queue:
                    response = b"-ERR MULTI calls can not be nested\r\n"
                else:
                    transaction_queue[client_socket] = []
        elif command_name == "EXEC":
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
        elif command_name == "DISCARD":
            if client_socket is None:
                response = b"-ERR DISCARD without MULTI\r\n"
            elif client_socket not in transaction_queue:
                response = b"-ERR DISCARD without MULTI\r\n"
            else:
                del transaction_queue[client_socket]
                response = b"+OK\r\n"
        else:
            response = b"-ERR unknown command\r\n"

    except Exception as e:
        response = f"-ERR {e}\r\n".encode()

    return response


def close_client_socket(s, inputs, outputs, recv_buffer, send_queue):
    global blocked_xread_requests

    blocked_client_deadline.pop(s, None)
    replica_connections.discard(s)
    replica_ack_offsets.pop(s, None)
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
    client_last_write_offset.pop(s, None)
    pending_wait_requests.pop(s, None)
    remove_client_from_all_subscriptions(s)
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
    upstream_node_id = None

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
            return None, b"", None

        master_socket.sendall(encode_resp_array(["REPLCONF", "listening-port", str(listening_port)]))
        replconf_port_response = read_line_from_master()
        if replconf_port_response != b"+OK":
            print(f"Unexpected response from master: {replconf_port_response.decode(errors='replace')}")
            master_socket.close()
            return None, b"", None

        master_socket.sendall(encode_resp_array(["REPLCONF", "capa", "psync2"]))
        replconf_capa_response = read_line_from_master()
        if replconf_capa_response != b"+OK":
            print(f"Unexpected response from master: {replconf_capa_response.decode(errors='replace')}")
            master_socket.close()
            return None, b"", None

        master_socket.sendall(encode_resp_array(["PSYNC", "?", "-1"]))
        psync_response = read_line_from_master()
        if not psync_response.startswith(b"+FULLRESYNC"):
            print(f"Unexpected response from master: {psync_response.decode(errors='replace')}")
            master_socket.close()
            return None, b"", None
        psync_parts = psync_response.decode(errors="replace").split()
        if len(psync_parts) >= 2:
            upstream_node_id = psync_parts[1]

        rdb_header = read_line_from_master()
        if not rdb_header.startswith(b"$"):
            print(f"Unexpected RDB header from master: {rdb_header.decode(errors='replace')}")
            master_socket.close()
            return None, b"", None

        rdb_length = int(rdb_header[1:])
        if rdb_length < 0:
            print("Unexpected negative RDB length from master")
            master_socket.close()
            return None, b"", None

        rdb_payload = read_exact_from_master(rdb_length)
        storage.clear()
        expire_times.clear()
        load_rdb_bytes(rdb_payload, storage, expire_times)
    except (ConnectionError, OSError, ValueError) as error:
        print(f"Unexpected response from master: {error}")
        master_socket.close()
        return None, b"", None

    return master_socket, handshake_buffer, upstream_node_id

def main():
    port = 6379
    role = "master"
    master_host = None
    master_port = None
    config_dir = "."
    config_dbfilename = "dump.rdb"
    appendonly = False
    appendfilename = "appendonly.aof"
    appendfsync = "everysec"
    auto_failover = False
    failover_timeout_ms_config = 10000
    failover_quorum_config = 1
    failover_peers_config = []
    save_rules = list(rdb_save_rules_default)
    bgsave_reap_mode = os.environ.get(
        "REDIS_BGSAVE_REAP_MODE",
        rdb_bgsave_reap_mode_default,
    ).lower()
    if "--port" in sys.argv:
        port_arg_index = sys.argv.index("--port")
        if port_arg_index + 1 < len(sys.argv):
            port = int(sys.argv[port_arg_index + 1])
    if "--dir" in sys.argv:
        dir_arg_index = sys.argv.index("--dir")
        if dir_arg_index + 1 < len(sys.argv):
            config_dir = sys.argv[dir_arg_index + 1]
    if "--dbfilename" in sys.argv:
        dbfilename_arg_index = sys.argv.index("--dbfilename")
        if dbfilename_arg_index + 1 < len(sys.argv):
            config_dbfilename = sys.argv[dbfilename_arg_index + 1]
    if "--appendonly" in sys.argv:
        appendonly_arg_index = sys.argv.index("--appendonly")
        if appendonly_arg_index + 1 < len(sys.argv):
            appendonly_value = sys.argv[appendonly_arg_index + 1].lower()
            if appendonly_value not in {"yes", "no"}:
                print("Invalid --appendonly value, expected 'yes' or 'no'")
                return
            appendonly = appendonly_value == "yes"
        else:
            print("Usage: --appendonly <yes|no>")
            return
    if "--appendfilename" in sys.argv:
        appendfilename_arg_index = sys.argv.index("--appendfilename")
        if appendfilename_arg_index + 1 < len(sys.argv):
            appendfilename = sys.argv[appendfilename_arg_index + 1]
        else:
            print("Usage: --appendfilename <filename>")
            return
    if "--appendfsync" in sys.argv:
        appendfsync_arg_index = sys.argv.index("--appendfsync")
        if appendfsync_arg_index + 1 < len(sys.argv):
            appendfsync = sys.argv[appendfsync_arg_index + 1].lower()
        else:
            print("Usage: --appendfsync <always|everysec|no>")
            return
    if appendfsync not in {"always", "everysec", "no"}:
        print("Invalid --appendfsync value, expected 'always', 'everysec', or 'no'")
        return
    if "--bgsave-reap-mode" in sys.argv:
        mode_arg_index = sys.argv.index("--bgsave-reap-mode")
        if mode_arg_index + 1 < len(sys.argv):
            bgsave_reap_mode = sys.argv[mode_arg_index + 1].lower()
        else:
            print("Usage: --bgsave-reap-mode <poll|sigchld>")
            return

    if "--auto-failover" in sys.argv:
        auto_failover_arg_index = sys.argv.index("--auto-failover")
        if auto_failover_arg_index + 1 < len(sys.argv):
            auto_failover_value = sys.argv[auto_failover_arg_index + 1].lower()
            if auto_failover_value not in {"yes", "no"}:
                print("Usage: --auto-failover <yes|no>")
                return
            auto_failover = auto_failover_value == "yes"
        else:
            print("Usage: --auto-failover <yes|no>")
            return

    if "--failover-timeout-ms" in sys.argv:
        failover_timeout_arg_index = sys.argv.index("--failover-timeout-ms")
        if failover_timeout_arg_index + 1 < len(sys.argv):
            try:
                failover_timeout_ms_config = int(sys.argv[failover_timeout_arg_index + 1])
            except ValueError:
                print("Usage: --failover-timeout-ms <positive-integer>")
                return
            if failover_timeout_ms_config <= 0:
                print("Usage: --failover-timeout-ms <positive-integer>")
                return
        else:
            print("Usage: --failover-timeout-ms <positive-integer>")
            return

    if "--failover-quorum" in sys.argv:
        failover_quorum_arg_index = sys.argv.index("--failover-quorum")
        if failover_quorum_arg_index + 1 < len(sys.argv):
            try:
                failover_quorum_config = int(sys.argv[failover_quorum_arg_index + 1])
            except ValueError:
                print("Usage: --failover-quorum <positive-integer>")
                return
            if failover_quorum_config <= 0:
                print("Usage: --failover-quorum <positive-integer>")
                return
        else:
            print("Usage: --failover-quorum <positive-integer>")
            return

    if "--failover-peers" in sys.argv:
        failover_peers_arg_index = sys.argv.index("--failover-peers")
        if failover_peers_arg_index + 1 < len(sys.argv):
            peers_raw = sys.argv[failover_peers_arg_index + 1].strip()
            if peers_raw:
                for peer_token in peers_raw.split(","):
                    peer = parse_peer_endpoint(peer_token.strip())
                    if peer is None:
                        print("Usage: --failover-peers <host1:port1,host2:port2,...>")
                        return
                    failover_peers_config.append(peer)
        else:
            print("Usage: --failover-peers <host1:port1,host2:port2,...>")
            return

    if bgsave_reap_mode not in {"poll", "sigchld"}:
        print("Invalid --bgsave-reap-mode value, expected 'poll' or 'sigchld'")
        return

    if "--save-rule" in sys.argv:
        save_rule_index = 0
        save_rules = []
        while save_rule_index < len(sys.argv):
            if sys.argv[save_rule_index] != "--save-rule":
                save_rule_index += 1
                continue
            if save_rule_index + 2 >= len(sys.argv):
                print("Usage: --save-rule <seconds> <changes>")
                return
            try:
                seconds_value = int(sys.argv[save_rule_index + 1])
                changes_value = int(sys.argv[save_rule_index + 2])
            except ValueError:
                print("Invalid --save-rule arguments, expected integers")
                return
            if seconds_value <= 0 or changes_value <= 0:
                print("Invalid --save-rule arguments, values must be > 0")
                return
            save_rules.append((seconds_value, changes_value))
            save_rule_index += 3
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
    global redis_config_dir
    redis_config_dir = config_dir
    global redis_config_dbfilename
    redis_config_dbfilename = config_dbfilename
    global upstream_master_host
    upstream_master_host = master_host
    global upstream_master_port
    upstream_master_port = master_port
    global upstream_connected
    upstream_connected = False
    global auto_failover_enabled
    auto_failover_enabled = auto_failover
    global failover_timeout_ms
    failover_timeout_ms = failover_timeout_ms_config
    global last_master_activity_at
    last_master_activity_at = datetime.now()
    global last_failover_reason
    last_failover_reason = ""
    global last_failover_at
    last_failover_at = None
    global failover_term
    failover_term = 0
    global node_id
    node_id = f"node-{port}-{os.getpid()}"
    global parent_node_id
    parent_node_id = None
    global pending_replicaof_target
    pending_replicaof_target = None
    global failover_quorum
    failover_quorum = failover_quorum_config
    global failover_peer_nodes
    failover_peer_nodes = [
        (host, peer_port)
        for host, peer_port in failover_peers_config
        if not (host in {"127.0.0.1", "localhost"} and peer_port == port)
    ]
    global election_voted_term
    election_voted_term = 0
    global election_voted_for
    election_voted_for = None
    global last_election_term
    last_election_term = 0
    global last_election_votes
    last_election_votes = 0
    global master_replid
    master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    global master_repl_offset
    master_repl_offset = 0
    global replica_repl_offset
    replica_repl_offset = 0
    global subscriptions
    subscriptions = {}
    
    global client_last_write_offset
    client_last_write_offset = {}
    global pending_wait_requests
    pending_wait_requests = {}

    global expire_times
    expire_times = {}
    global storage
    storage = {}
    load_rdb_file(redis_config_dir, redis_config_dbfilename, storage)
    global blocked_clients_by_key
    blocked_clients_by_key = {}
    global blocked_client_deadline
    blocked_client_deadline = {}
    global blocked_xread_requests
    blocked_xread_requests = []
    global replica_connections
    replica_connections = set()
    global replica_ack_offsets
    replica_ack_offsets = {}
    global commands_queue
    commands_queue = []
    global transaction_queue
    transaction_queue = {}
    global rdb_snapshot_dirty
    rdb_snapshot_dirty = False
    global rdb_save_rules
    rdb_save_rules = save_rules
    global rdb_dirty_version
    rdb_dirty_version = 0
    global rdb_changes_since_last_save
    rdb_changes_since_last_save = 0
    global last_rdb_save_time
    last_rdb_save_time = datetime.now()
    global rdb_bgsave_pid
    rdb_bgsave_pid = None
    global rdb_bgsave_version
    rdb_bgsave_version = 0
    global rdb_bgsave_changes_at_start
    rdb_bgsave_changes_at_start = 0
    global rdb_bgsave_reap_mode
    rdb_bgsave_reap_mode = bgsave_reap_mode
    global rdb_child_exit_pending
    rdb_child_exit_pending = False

    global aof_enabled
    aof_enabled = appendonly
    global aof_fsync_policy
    aof_fsync_policy = appendfsync
    global aof_pending_chunks
    aof_pending_chunks = []
    global aof_file
    aof_file = None
    global next_aof_fsync_at
    next_aof_fsync_at = datetime.now() + timedelta(seconds=aof_fsync_interval_seconds)

    if aof_enabled:
        aof_file_path = os.path.join(redis_config_dir, appendfilename)
        aof_file = open(aof_file_path, "ab")

    if rdb_bgsave_reap_mode == "sigchld":
        signal.signal(signal.SIGCHLD, handle_sigchld)
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    server_socket = socket.create_server(("127.0.0.1", port), reuse_port=True)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.setblocking(False)
    server_socket.listen(socket_queue_size)
    inputs = [server_socket]
    master_connection = None
    master_initial_buffer = b""
    upstream_node_id = None
    if server_role == "slave" and master_host is not None and master_port is not None:
        master_connection, master_initial_buffer, upstream_node_id = send_replica_ping(master_host, master_port, port)
        if master_connection is None:
            print("Failed to communicate with master, exiting.")
            return
        upstream_connected = True
        parent_node_id = upstream_node_id
        last_master_activity_at = datetime.now()
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

    def process_master_input_buffer(connection_socket):
        global replica_repl_offset

        if connection_socket not in recv_buffer:
            return
        try:
            while True:
                commands, remaining = parse_resp_from_buffer(recv_buffer[connection_socket])
                if commands is None:
                    recv_buffer[connection_socket] = remaining
                    break

                command_bytes_len = len(recv_buffer[connection_socket]) - len(remaining)
                recv_buffer[connection_socket] = remaining
                response = handle_client(commands, client_socket=connection_socket)
                is_getack_request = (
                    len(commands) == 3
                    and commands[0].upper() == "REPLCONF"
                    and commands[1].upper() == "GETACK"
                    and commands[2] == "*"
                )
                if not is_getack_request:
                    response = None
                else:
                    response = encode_resp_array(["REPLCONF", "ACK", str(replica_repl_offset)])
                replica_repl_offset += command_bytes_len

                if response is not None:
                    send_queue[connection_socket].append(response)
                    if connection_socket not in outputs:
                        outputs.append(connection_socket)
                    flush_send_queue_for_socket(connection_socket)
        except ValueError:
            recv_buffer[connection_socket] = b""

    def apply_pending_replicaof_change():
        nonlocal master_connection
        global server_role
        global upstream_master_host
        global upstream_master_port
        global upstream_connected
        global parent_node_id
        global pending_replicaof_target
        global last_master_activity_at

        if pending_replicaof_target is None:
            return

        target_host, target_port = pending_replicaof_target
        pending_replicaof_target = None

        if master_connection is not None:
            close_client_socket(master_connection, inputs, outputs, recv_buffer, send_queue)
            master_connection = None

        if target_host is None or target_port is None:
            promote_to_master("REPLICAOF NO ONE")
            return

        server_role = "slave"
        upstream_master_host = target_host
        upstream_master_port = target_port
        upstream_connected = False

        new_master_connection, initial_buffer, new_parent_node_id = send_replica_ping(
            target_host,
            target_port,
            port,
        )
        if new_master_connection is None:
            print(f"Failed to switch replication upstream to {target_host}:{target_port}")
            return

        master_connection = new_master_connection
        master_connection.setblocking(False)
        inputs.append(master_connection)
        recv_buffer[master_connection] = initial_buffer
        send_queue[master_connection] = []
        upstream_connected = True
        parent_node_id = new_parent_node_id
        last_master_activity_at = datetime.now()
        process_master_input_buffer(master_connection)

    if master_connection is not None:
        recv_buffer[master_connection] = master_initial_buffer
        send_queue[master_connection] = []
        if master_initial_buffer:
            last_master_activity_at = datetime.now()
            process_master_input_buffer(master_connection)

    try:
        while True:
            readable, writable, exceptional = select.select(
                inputs, outputs, inputs, socket_timeout)

            now = datetime.now()
            maybe_reap_bgsave_status(now)
            maybe_start_bgsave(now)
            flush_aof_if_needed(now)
            apply_pending_replicaof_change()

            if (
                auto_failover_enabled
                and server_role == "slave"
                and master_connection is not None
                and now >= last_master_activity_at + timedelta(milliseconds=failover_timeout_ms)
            ):
                close_client_socket(master_connection, inputs, outputs, recv_buffer, send_queue)
                attempt_failover_election(
                    f"master timeout after {failover_timeout_ms}ms"
                )
                master_connection = None

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
            for wait_client, request in list(pending_wait_requests.items()):
                acked_count = sum(1 for replica in replica_connections if replica_ack_offsets.get(replica, 0) >= request["target_offset"])
                if acked_count >= request["target_replicas"] or datetime.now() >= request["deadline"]:
                    send_queue[wait_client].append(f":{acked_count}\r\n".encode())
                    if wait_client not in outputs:
                        outputs.append(wait_client)
                    pending_wait_requests.pop(wait_client, None)
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
                        if master_connection is not None and s is master_connection:
                            last_master_activity_at = datetime.now()
                        recv_buffer[s] += data

                        try:
                            while True:
                                commands, remaining = parse_resp_from_buffer(recv_buffer[s])
                                if commands is None:
                                    recv_buffer[s] = remaining
                                    break

                                command_bytes_len = len(recv_buffer[s]) - len(remaining)
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
                                        else:
                                            response = encode_resp_array(["REPLCONF", "ACK", str(replica_repl_offset)])
                                        replica_repl_offset += command_bytes_len
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
                        if master_connection is not None and s is master_connection:
                            if auto_failover_enabled:
                                promote_to_master("master connection closed")
                                master_connection = None
                            else:
                                upstream_connected = False
                        close_client_socket(s, inputs, outputs, recv_buffer, send_queue)
            for s in writable:
                if s in send_queue and send_queue[s]:
                    try:
                        flush_send_queue_for_socket(s)
                    except Exception as e:
                        print(f"处理客户端 {s.getpeername()} 时出错: {e}")
                        if master_connection is not None and s is master_connection:
                            if auto_failover_enabled:
                                promote_to_master("master write error")
                                master_connection = None
                            else:
                                upstream_connected = False
                        close_client_socket(s, inputs, outputs, recv_buffer, send_queue)

            for s in exceptional:
                if master_connection is not None and s is master_connection:
                    if auto_failover_enabled:
                        promote_to_master("master socket exception")
                        master_connection = None
                    else:
                        upstream_connected = False
                close_client_socket(s, inputs, outputs, recv_buffer, send_queue)
            
    except Exception as e:
        print(f"服务器发生错误: {e}")


if __name__ == "__main__":
    main()
