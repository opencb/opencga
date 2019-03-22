import unittest
from mount import get_avere_ips, get_ip_address, ip_as_int

class TestAvere(unittest.TestCase):
    def test_get_avere_ips(self):
        res = get_avere_ips("10.0.1.10-10.0.1.15")
        self.assertEqual(res, ["10.0.1.10", "10.0.1.11", "10.0.1.12", "10.0.1.13", "10.0.1.14", "10.0.1.15"])

    def test_get_ip(self):
        res = get_ip_address()
        self.assertIsNotNone(res)

    
    def test_ip2int(self):
        res = ip_as_int("10.0.1.174")
        self.assertEqual(res, 167772590)


if __name__ == '__main__':
    unittest.main()