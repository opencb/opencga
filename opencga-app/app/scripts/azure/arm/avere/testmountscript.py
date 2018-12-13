import unittest
from mount import get_avere_ips

class TestAvere(unittest.TestCase):
    def test_get_avere_ips(self):
        res = get_avere_ips("10.0.1.10-10.0.1.15")
        self.assertEqual(res, ["10.0.1.10", "10.0.1.11", "10.0.1.12", "10.0.1.13", "10.0.1.14", "10.0.1.15"])


if __name__ == '__main__':
    unittest.main()