import unittest
from hashring import MD5HashRing

class TestHashRing(unittest.TestCase):
    def setUp(self):
        self.base_servers = ['localhost:{}'.format(i) for i in range(10)]
        self.ring = MD5HashRing(self.base_servers)

    def test_cache_store(self):
        cache_urls = ['fb.com/{}'.format(i) for i in range(50)]
        cache_urls2 = ['bf.com/{}'.format(i) for i in range(50)]

        for url in cache_urls:
            self.ring.add_cache(url)
        
        cache_servers = [self.ring.get_cache_server(url) for url in cache_urls]

        for url in cache_urls2:
            self.ring.add_cache(url)

        cache_servers_after_adding = [self.ring.get_cache_server(url) for url in cache_urls]

        self.assertEqual(cache_servers, cache_servers_after_adding)
    
    def test_add_cache_server(self):
        self.ring = MD5HashRing(self.base_servers)
        cache_urls = ['fb.com/{}'.format(i) for i in range(50)]

        for url in cache_urls:
            self.ring.add_cache(url)

        new_servers = ['REMOThost:{}'.format(i) for i in range(10)]
        self.ring.add_cache_servers(new_servers)

        servers = []
        for url in cache_urls:
            servers.append(self.ring._get_clockwise_cache_server(url))

        cache_servers = [self.ring.get_cache_server(url) for url in cache_urls]

        self.assertEqual(servers, cache_servers)

if __name__ == '__main__':
    unittest.main()