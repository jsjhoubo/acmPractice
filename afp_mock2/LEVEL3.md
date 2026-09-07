# Level 3 — Payments with cashback

- `pay(timestamp, account_id, amount)` -> str | None — withdraw `amount` from
  the account. Returns a payment id: "payment1", "payment2", ... (one global
  counter across all accounts, in order of successful pay calls). Returns
  None if the account does not exist or has insufficient funds. A successful
  pay counts toward the account's outgoing total (Level 2).
  Each payment earns **2% cashback, rounded down** (amount * 2 // 100),
  credited back to the account **exactly 86400 seconds after** the pay
  timestamp.
- Cashback processing is lazy: at the start of ANY operation with timestamp
  `t`, first credit every pending cashback whose due time <= t.
- `get_payment_status(timestamp, account_id, payment_id)` -> str | None —
  "IN_PROGRESS" before the cashback lands, "CASHBACK_RECEIVED" after.
  None if the account doesn't exist, the payment doesn't exist, or the
  payment belongs to a different account.

Run: python tests/level3_test.py
