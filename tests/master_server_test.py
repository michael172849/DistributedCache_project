import requests
import unittest
unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: x < y

class TestMasterServer(unittest.TestCase):
    def setUp(self):
        self.url = "http://0.0.0.0:8080/"
        self.kv_url = self.url + "kv"

    def test_1_post(self):
        payload = {'key': 'test_key', 'value': 'test_val'}
        print("POST " + str(payload))
        r = requests.post(self.kv_url, params=payload)
        print(r.url)
        print(r.text)

    def test_2_get(self):
        payload = {'key': 'test_key'}
        print("GET " + str(payload))
        r = requests.get(self.kv_url, params=payload)
        print(r.url)
        print(r.text)

if __name__ == '__main__':
    unittest.main()