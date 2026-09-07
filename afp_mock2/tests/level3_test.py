import sys, os, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from solution import Bank

DAY = 86400

class TestLevel3(unittest.TestCase):
    def test_pay_and_cashback(self):
        b = Bank()
        b.create_account(1, "a")
        b.deposit(2, "a", 100)
        self.assertEqual(b.pay(3, "a", 60), "payment1")     # balance 40, cashback 1 due at 3+DAY
        self.assertEqual(b.get_payment_status(4, "a", "payment1"), "IN_PROGRESS")
        self.assertEqual(b.get_payment_status(3 + DAY - 1, "a", "payment1"), "IN_PROGRESS")
        # deposit at due time: cashback (60*2//100 = 1) lands first, then +10
        self.assertEqual(b.deposit(3 + DAY, "a", 10), 51)
        self.assertEqual(b.get_payment_status(3 + DAY + 1, "a", "payment1"), "CASHBACK_RECEIVED")

    def test_pay_failures_and_ids(self):
        b = Bank()
        b.create_account(1, "a"); b.deposit(2, "a", 100)
        self.assertIsNone(b.pay(3, "a", 1000))
        self.assertIsNone(b.pay(4, "zz", 5))
        self.assertEqual(b.pay(5, "a", 10), "payment1")
        self.assertEqual(b.pay(6, "a", 10), "payment2")     # global increasing ids
        self.assertIsNone(b.get_payment_status(7, "a", "payment9"))
        b.create_account(8, "b")
        self.assertIsNone(b.get_payment_status(9, "b", "payment1"))  # not b's payment

    def test_pay_counts_as_spending(self):
        b = Bank()
        b.create_account(1, "a"); b.deposit(2, "a", 100)
        b.pay(3, "a", 40)
        self.assertEqual(b.top_spenders(4, 1), "a(40)")

if __name__ == "__main__":
    unittest.main()
