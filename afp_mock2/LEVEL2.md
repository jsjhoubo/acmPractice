# Level 2 — Top spenders

Track, for every account, its total **outgoing** amount (money leaving the
account via `transfer`; later levels may add more outgoing kinds).

- `top_spenders(timestamp, n)` -> str — the top `n` accounts by total
  outgoing, formatted `"id1(total1), id2(total2), ..."`, sorted by total
  descending; ties broken by account id ascending. Accounts with zero
  outgoing are included. If fewer than `n` accounts exist, list them all.

Run: python tests/level2_test.py
