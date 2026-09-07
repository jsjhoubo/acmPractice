import sys, os, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from solution import Bank

class TestLevel2(unittest.TestCase):
    def make(self):
        b = Bank()
        b.create_account(1, "a"); b.create_account(2, "b"); b.create_account(3, "c")
        b.deposit(4, "a", 200); b.deposit(5, "b", 100); b.deposit(6, "c", 50)
        b.transfer(7, "a", "b", 100)   # a spent 100
        b.transfer(8, "b", "c", 30)    # b spent 30
        b.transfer(9, "c", "a", 30)    # c spent 30
        return b

    def test_top_spenders_sorted_ties(self):
        b = self.make()
        self.assertEqual(b.top_spenders(10, 3), "a(100), b(30), c(30)")

    def test_top_n_smaller_and_larger(self):
        b = self.make()
        self.assertEqual(b.top_spenders(10, 1), "a(100)")
        self.assertEqual(b.top_spenders(10, 5), "a(100), b(30), c(30)")

    def test_zero_spender_included(self):
        b = Bank()
        b.create_account(1, "x"); b.create_account(2, "y")
        b.deposit(3, "x", 10); b.transfer(4, "x", "y", 5)
        self.assertEqual(b.top_spenders(5, 2), "x(5), y(0)")

if __name__ == "__main__":
    unittest.main()
