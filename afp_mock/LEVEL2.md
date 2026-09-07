# Level 2 — Scanning

The database should now support displaying the contents of a record.

- `scan(key)` — return a string of all fields in record `key`, formatted as
  `"field1(value1), field2(value2), ..."` where fields appear in
  **lexicographically ascending order**, joined by a comma and a space.
  If the record does not exist or has no fields, return `""` (empty string).
- `scan_by_prefix(key, prefix)` — same format, but include only fields whose
  name **starts with** `prefix`. Missing record or no matching fields → `""`.

Run: `python tests/level2_test.py` (level 1 tests must still pass).
