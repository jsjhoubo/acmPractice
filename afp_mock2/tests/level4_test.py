import sys, os, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from solution import Bank

class TestLevel4(unittest.TestCase):
    def make(self):
        b = Bank()
        b.create_account(1, "a"); b.create_account(2, "b")
        b.deposit(3, "a", 100); b.deposit(4, "b", 50)
        b.transfer(5, "a", "b", 20)          # a=80(spent 20), b=70
        return b

    def test_merge_and_after(self):
        b = self.make()
        self.assertFalse(b.merge_accounts(6, "b", "zz"))
        self.assertFalse(b.merge_accounts(7, "b", "b"))
        self.assertTrue(b.merge_accounts(8, "b", "a"))     # a INTO b: b=150, b spending 20
        self.assertIsNone(b.deposit(9, "a", 10))            # a is gone
        self.assertEqual(b.top_spenders(10, 2), "b(20)")

    def test_get_balance_history(self):
        b = self.make()
        b.merge_accounts(6, "b", "a")
        self.assertEqual(b.get_balance(9, "a", 5), 80)      # a at t=5
        self.assertEqual(b.get_balance(9, "a", 3), 100)     # a right after its deposit
        self.assertIsNone(b.get_balance(9, "a", 6))         # merged away at 6
        self.assertEqual(b.get_balance(9, "b", 6), 150)
        self.assertEqual(b.get_balance(9, "b", 3), 0)       # b existed, balance 0
        self.assertIsNone(b.get_balance(9, "zz", 5))
        self.assertIsNone(b.get_balance(9, "b", 1))         # b created at 2

if __name__ == "__main__":
    unittest.main()
