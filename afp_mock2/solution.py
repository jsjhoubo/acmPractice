class Bank:
    def __init__(self):
        self._account ={}
        self._outgoing ={}
        self._cashback ={}
        self._payment ={}
        self._totalpay =0
        self._ops = []
    
    def process_cashback(self, timestamp: int, account_id:str) :
        if self._cashback.get(account_id) is None:
            return
        if self._account.get(account_id) is None:
            return 
        all_processed =[]
        for payid, (t, value) in self._cashback[account_id].items():
            if t + 86400 <= timestamp:
                all_processed.append(payid)
                self._account[account_id] += value
        for id in all_processed:
            self._cashback[account_id].pop(id)

    # ---- Level 1 ----
    def create_account(self, timestamp: int, account_id: str) -> bool:
        """Create account. False if it already exists."""
        """create a new account with
  balance 0. Returns False (and does nothing) if the account already exists."""
        if self._account.get(account_id) is not None:
            return False
        self._account[account_id] =0
        self._outgoing[account_id] =0
        self._payment[account_id] ={}
        self._cashback[account_id] ={}
        self._ops.append(("create", timestamp, account_id))
        return True

    def deposit(self, timestamp: int, account_id: str, amount: int) ->int | None:
        """add `amount` to the
  account's balance and return the new balance, or None if the account does
  not exist."""
        self.process_cashback(timestamp, account_id)
        if self._account.get(account_id) is None:
            return None
        self._account[account_id] += amount
        self._ops.append(("deposit", timestamp, account_id, amount))
        return self._account[account_id]

    def transfer(self, timestamp: int, source: str, target: str, amount: int)-> int | None :
        """move
  `amount` from source to target and return the **source's** new balance.
  Return None (and change nothing) if either account does not exist, if
  source and target are the same account, or if source's balance < amount."""
        self.process_cashback(timestamp, source)
        self.process_cashback(timestamp, target)
        if self._account.get(source) is None or self._account.get(target) is None or source == target:
            return None
        if self._account[source] < amount :
            return None
        self._account[source] -= amount
        self._outgoing[source] += amount
        self._account[target] += amount
        self._ops.append(("transfer", timestamp, source, target, amount))
        return self._account[source]

    # ---- Level 2+ : add methods as you unlock each level ----
    def top_spenders(self, timestamp: int, n: int) -> str : 
        """— the top `n` accounts by total
  outgoing, formatted `"id1(total1), id2(total2), ..."`, sorted by total
  descending; ties broken by account id ascending. Accounts with zero
  outgoing are included. If fewer than `n` accounts exist, list them all."""
        ret =''
        cnt =0
        data =[]
        for account in self._account.keys():
            self.process_cashback(timestamp, account)
        for id, money in sorted(self._outgoing.items(), key=lambda x: (-x[1], x[0])):
            data.append(str(id)+'(' +str(money)+')')
            cnt =cnt+1
            if cnt >=n:
                break
        return ', '.join(data)
        
    def pay(self, timestamp:int, account_id:str, amount: int) -> str | None:
        """withdraw `amount` from
  the account. Returns a payment id: "payment1", "payment2", ... (one global
  counter across all accounts, in order of successful pay calls). Returns
  None if the account does not exist or has insufficient funds. A successful
  pay counts toward the account's outgoing total (Level 2).
  Each payment earns **2% cashback, rounded down** (amount * 2 // 100),
  credited back to the account **exactly 86400 seconds after** the pay
  timestamp.
- Cashback processing is lazy: at the start of ANY operation with timestamp
  `t`, first credit every pending cashback whose due time <= t."""
        self.process_cashback(timestamp, account_id)
        if self._account.get(account_id) is None:
            return None
        
        if self._account[account_id] < amount:
            return None
        self._account[account_id] -= amount
        self._outgoing[account_id] += amount
        self._totalpay +=1
        payid ='payment' + str(self._totalpay)
        self._payment[account_id][payid] =amount
        self._cashback[account_id][payid] =(timestamp, amount *2//100)
        self._ops.append(("pay", timestamp, account_id, amount))
        
        return payid

    
    def get_payment_status(self, timestamp: int, account_id:str, payment_id:str)-> str | None:
        """
  "IN_PROGRESS" before the cashback lands, "CASHBACK_RECEIVED" after.
  None if the account doesn't exist, the payment doesn't exist, or the
  payment belongs to a different account.    
  """
        self.process_cashback(timestamp, account_id)
        if self._account.get(account_id) is None:
            return None
        if self._payment[account_id].get(payment_id) is None:
            return None
        if self._cashback[account_id].get(payment_id) is not None and self._cashback[account_id][payment_id][0] + 86400 > timestamp :
            return "IN_PROGRESS"
        return "CASHBACK_RECEIVED"


# Level 4 — Merging accounts and historical balance

    def merge_accounts(self, timestamp: int, account_id_1:str, account_id_2:str) -> bool :
        """— merge
  account 2 INTO account 1: balances add up, outgoing totals add up, account
  2's payments and pending cashbacks now belong to account 1, and account 2
  ceases to exist from this timestamp on. Returns False (no change) if the
  ds are equal or either account doesn't exist."""
        self.process_cashback(timestamp, account_id_1)
        self.process_cashback(timestamp, account_id_2)
        if self._account.get(account_id_1) is None or self._account.get(account_id_2) is None:
            return False
        if account_id_2 == account_id_1:
            return False
        self._account[account_id_1] +=self._account[account_id_2]
        self._account.pop(account_id_2)
        self._outgoing[account_id_1] += self._outgoing[account_id_2]
        self._outgoing.pop(account_id_2)
        for payid, amount in self._payment[account_id_2].items():
            self._payment[account_id_1][payid] =amount
        self._payment.pop(account_id_2)
        if self._cashback.get(account_id_2) is not None:
            for payid, data in self._cashback[account_id_2].items():
                    self._cashback[account_id_1][payid] = data
            self._cashback.pop(account_id_2)
        self._ops.append(("merge", timestamp, account_id_1, account_id_2))
        return True
                         
    def get_balance(self, timestamp, account_id, time_at) -> int | None :
        """ — the balance
  of `account_id` as it was at time `time_at` (after all operations with
  timestamp <= time_at, including cashbacks due by then). Return None if the
  account did not exist at `time_at` (not yet created, or already merged
  away)."""
        sim_accounts = {}
        sim_cashback = {}

        def process_due(ts):
            for acc in list(sim_cashback.keys()):
                pending = sim_cashback[acc]
                keep = []
                for due_ts, value in pending:
                    if due_ts <= ts and acc in sim_accounts:
                        sim_accounts[acc] += value
                    else:
                        keep.append((due_ts, value))
                sim_cashback[acc] = keep

        for op in self._ops:
            op_type = op[0]
            op_ts = op[1]
            if op_ts > time_at:
                continue

            process_due(op_ts)

            if op_type == "create":
                _, _, acc = op
                if acc not in sim_accounts:
                    sim_accounts[acc] = 0
                    sim_cashback[acc] = []
            elif op_type == "deposit":
                _, _, acc, amount = op
                if acc in sim_accounts:
                    sim_accounts[acc] += amount
            elif op_type == "transfer":
                _, _, source, target, amount = op
                if source in sim_accounts and target in sim_accounts and source != target and sim_accounts[source] >= amount:
                    sim_accounts[source] -= amount
                    sim_accounts[target] += amount
            elif op_type == "pay":
                _, _, acc, amount = op
                if acc in sim_accounts and sim_accounts[acc] >= amount:
                    sim_accounts[acc] -= amount
                    sim_cashback.setdefault(acc, []).append((op_ts + 86400, amount * 2 // 100))
            elif op_type == "merge":
                _, _, dst, src = op
                if dst in sim_accounts and src in sim_accounts and dst != src:
                    sim_accounts[dst] += sim_accounts[src]
                    sim_accounts.pop(src)
                    sim_cashback.setdefault(dst, []).extend(sim_cashback.get(src, []))
                    if src in sim_cashback:
                        sim_cashback.pop(src)

        process_due(time_at)
        return sim_accounts.get(account_id)
        
