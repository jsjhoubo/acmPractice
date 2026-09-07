# Level 1 — Accounts and transfers

The banking system should support creating accounts, depositing money, and
transferring between accounts. All operations carry an integer `timestamp`
(seconds); timestamps across calls are strictly increasing.

- `create_account(timestamp, account_id)` -> bool — create a new account with
  balance 0. Returns False (and does nothing) if the account already exists.
- `deposit(timestamp, account_id, amount)` -> int | None — add `amount` to the
  account's balance and return the new balance, or None if the account does
  not exist.
- `transfer(timestamp, source_id, target_id, amount)` -> int | None — move
  `amount` from source to target and return the **source's** new balance.
  Return None (and change nothing) if either account does not exist, if
  source and target are the same account, or if source's balance < amount.

Run: python tests/level1_test.py
