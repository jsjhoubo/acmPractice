# Level 3 — Timestamps and TTL

All new operations carry an integer `timestamp` (seconds). Timestamps passed
to **write** operations are guaranteed to be strictly increasing across calls.
Existing Level 1/2 methods remain and must keep working.

- `set_at(key, field, value, timestamp)` — like `set`, at the given time.
  The field never expires.
- `set_at_with_ttl(key, field, value, timestamp, ttl)` — like `set_at`, but
  the field is only alive during `[timestamp, timestamp + ttl)`. Setting a
  field again (with or without TTL) completely replaces its value AND its
  expiration behavior.
- `delete_at(key, field, timestamp)` — delete the field if it is alive at
  `timestamp`; return True if deleted, False otherwise (missing or expired).
- `get_at(key, field, timestamp)` — return the value if the field is alive at
  `timestamp`, else None. A field with TTL is alive at `t` iff
  `set_time <= t < set_time + ttl`.
- `scan_at(key, timestamp)` / `scan_by_prefix_at(key, prefix, timestamp)` —
  Level 2 formatting, but only fields alive at `timestamp` are included.

Run: `python tests/level3_test.py`.
