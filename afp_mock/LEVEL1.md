# Level 1 — Basic operations

Your task is to implement a simplified in-memory database. Records are
identified by a string `key`; each record holds fields, each field a string
value. At this level the database should support the following operations:

- `set(key, field, value)` — insert a field-value pair into the record
  associated with `key`. If the record does not exist, it is created. If the
  field already exists, its value is overwritten.
- `get(key, field)` — return the value of `field` in record `key`, or `None`
  if the record or the field does not exist.
- `delete(key, field)` — remove `field` from record `key`. Returns `True` if
  the field existed and was removed, `False` otherwise (including when the
  record itself does not exist).

Run: `python tests/level1_test.py` — all green before opening LEVEL2.md.
