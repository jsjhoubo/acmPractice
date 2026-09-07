import sys, os, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from solution import Bank

class TestLevel1(unittest.TestCase):
    def test_create(self):
        b = Bank()
        self.assertTrue(b.create_account(1, "a"))
        self.assertFalse(b.create_account(2, "a"))

    def test_deposit(self):
        b = Bank()
        b.create_account(1, "a")
        self.assertEqual(b.deposit(2, "a", 100), 100)
        self.assertEqual(b.deposit(3, "a", 50), 150)
        self.assertIsNone(b.deposit(4, "zz", 10))

    def test_transfer(self):
        b = Bank()
        b.create_account(1, "a"); b.create_account(2, "b")
        b.deposit(3, "a", 100); b.deposit(4, "b", 50)
        self.assertEqual(b.transfer(5, "a", "b", 30), 70)
        self.assertEqual(b.deposit(6, "b", 0), 80)
        self.assertIsNone(b.transfer(7, "a", "b", 1000))
        self.assertIsNone(b.transfer(8, "a", "a", 10))
        self.assertIsNone(b.transfer(9, "a", "zz", 5))
        self.assertIsNone(b.transfer(10, "zz", "a", 5))

if __name__ == "__main__":
    unittest.main()
