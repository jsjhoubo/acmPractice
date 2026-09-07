# Level 4 — Merging accounts and historical balance

- `merge_accounts(timestamp, account_id_1, account_id_2)` -> bool — merge
  account 2 INTO account 1: balances add up, outgoing totals add up, account
  2's payments and pending cashbacks now belong to account 1, and account 2
  ceases to exist from this timestamp on. Returns False (no change) if the
  ids are equal or either account doesn't exist.
- `get_balance(timestamp, account_id, time_at)` -> int | None — the balance
  of `account_id` as it was at time `time_at` (after all operations with
  timestamp <= time_at, including cashbacks due by then). Return None if the
  account did not exist at `time_at` (not yet created, or already merged
  away).

Run: python tests/level4_test.py — 全绿记录用时。
