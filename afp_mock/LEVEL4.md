# Level 4 — Backup and restore

- `backup(timestamp)` — save a snapshot of the database state at `timestamp`
  (only fields alive at that moment are part of the snapshot). Returns the
  number of records that have at least one alive field at `timestamp`.
- `restore(timestamp)` — replace the current database state with the snapshot
  from the **latest backup taken at a time <= `timestamp`** (a valid backup is
  guaranteed to exist). For every restored field that had a TTL, its remaining
  time-to-live is preserved: if at backup time the field had `R` seconds of
  life left, then after the restore it expires at `timestamp + R`. Fields
  without TTL are restored without TTL.

Run: `python tests/level4_test.py`. All four levels green = done — note your
finish time.
