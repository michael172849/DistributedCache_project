from hashlib import md5

class InconsistentHash:
    def __init__(self, cache_servers):
        self.num_cache_servers = len(cache_servers)
        self.cache_servers = cache_servers

    def _get_cache_server(self, url):
        if self.num_cache_servers == 0:
            return "GET_CACHE_SERVER ERROR"
        return self.cache_servers[int(self._hash_url(url) / (self._max_digest_val / self.num_cache_servers))]

    def add_single_cache_server(self, cache_server):
        self.num_cache_servers += 1
        self.cache_servers.append(cache_server)

    def remove_cache_server(self, cache_server):
        self.num_cache_servers -= 1
        self.cache_servers.remove(cache_server)

class InconsistentMd5Hash(InconsistentHash):
    def __init__(self, cache_servers):
        self._max_digest_val = 2 ** 128 - 1
        self._hash_func = md5
        super().__init__(cache_servers)

    def _hash_url(self, url):
        m = self._hash_func()
        m.update(url.encode())
        return int(m.hexdigest(), 16)