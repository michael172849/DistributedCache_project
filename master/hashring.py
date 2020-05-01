from bintrees import AVLTree
from hashlib import md5
from numpy import log2
import numpy as np
import random
# maps URI to the cache server id
# implements https://www.cs.princeton.edu/courses/archive/fall07/cos518/papers/chash.pdf
DEBUG = True

# for testing
if DEBUG:
    random.seed(23333)

class HashRing:
    """HashRing abstract class. Please use the implementation version of this class
    """
    def __init__(self, cache_servers):
        """
        # args:
               cache_servers: list of urls for cache servers
        """
        # Tree that maps buckets to cache servers. Represents the whole hashring.
        self._val_to_serv_url = AVLTree()  

        self._n = np.ceil(log2(len(cache_servers)))

        # divide the hash ring into 2^n intervals
        self._num_buckets = np.power(2, self._n)
        # make interval * (2 ^ n) is slightly bigger than the max_digest_val
        self._interval = int(np.ceil(self._max_digest_val / self._num_buckets))
        self._buckets = [i for i in range(0, self._max_digest_val, self._interval)]

        # one balancing binary tree is used per bucket to store urls. Represents single cache server 
        # an element in a tree maps hashed url to url itself
        self._cache_trees = dict([(serv, AVLTree()) for serv in cache_servers])

        # randomly allocate cache servers to buckets
        self._add_servers_to_remaining_buckets(cache_servers)

        if DEBUG:
            print(self._val_to_serv_url)


    def _get_cache_server_url(self, url):
        return self._val_to_serv_url.ceiling_item(self._hash_url(url))[1]  # get the cache server that is clockwise
        
    def _get_clockwise_cache_server(self, val):
        return self._val_to_serv_url.ceiling_item(val)[1]

    def get_cache_server(self, url):
        """returns a cache server in which the cache is/will be stored
        raises KeyError if url is not found @TODO how to handle this?
        """
        server_url = self._get_cache_server_url(url)
        self._cache_trees[server_url][self._hash_url(url)]
        return server_url

    def add_cache(self, url):
        """add a cache
        """
        server_url = self._get_cache_server_url(url)
        self._cache_trees[server_url].insert(self._hash_url(url), url)
        # @TODO callback for adding a cache to cache server
    
    def remove_cache(self, url):
        """remove cache
        """
        server_url = self._get_cache_server_url(url)
        try:
            self._cache_trees[server_url].remove(self._hash_url(url))
            # @TODO callback for removing a cache from cache server
            return True
        except:
            return False

    def _bisect_segments(self, num_new_buckets):
        """bisect the segments on the hashring so that total number of buckets will be 2^(n+1)
        """
        new_n = np.ceil(log2(len(self._val_to_serv_url) + num_new_buckets))
        new_interval = int(np.ceil(self._max_digest_val / np.power(2, new_n)))
        
        for i in range(self._interval, self._max_digest_val, self._interval):
            for j in range(i, i+self._interval, new_interval):
                self._buckets.append(j)
        
        self._interval = new_interval

    def _add_servers_to_remaining_buckets(self, cache_servers):
        """this function allocates cache servers to remaining buckets as many as it can
        """
        random.shuffle(cache_servers)
        random.shuffle(self._buckets)
        for i in range(len(self._buckets)):
            if i >= len(cache_servers):
                break
            try:
                old_cache_server = self._cache_trees[self._get_clockwise_cache_server(self._buckets[i])]
            except KeyError:
                old_cache_server = dict()
            self._val_to_serv_url.insert(self._buckets[i], cache_servers[i])
            new_cache_server = AVLTree()
            for hashed_url in old_cache_server.keys():
                if hashed_url < self._buckets[i]:
                    new_cache_server.insert(hashed_url, old_cache_server[hashed_url])
                    # @TODO callback to send cache to new server
                    old_cache_server.remove(hashed_url)
                    # @TODO callback to invalidate old cache here

            self._cache_trees[cache_servers[i]] = new_cache_server

        self._buckets = self._buckets[i+1:]  # remaining buckets
        return cache_servers[i+1:]

    def add_cache_servers(self, cache_servers):
        """adds a cache server to hash ring
        """
        # use up all the buckets to add new cache servers
        rem_cache_servers = self._add_servers_to_remaining_buckets(cache_servers)
        
        if rem_cache_servers and not self._buckets:
            if DEBUG:
                print("adding buckets")
            self._bisect_segments(len(rem_cache_servers))
            rem_cache_servers = self._add_servers_to_remaining_buckets(rem_cache_servers)
            if rem_cache_servers:
                raise Error("something wrong")

    def remove_cache_server(self, server_url):
        for val, url in self._val_to_serv_url.items():
            if url == server_url:
                del self._val_to_serv_url[val]
        # @TODO callback to invalidate all the cache
        old_cache_server = self._cache_trees[server_url]
        self._cache_trees[self.get_cache_server(server_url)].update(old_cache_server)
        # @TODO callback to send the cache to new cache server
        del self.cache_trees[server_url]
            
    def _hash_url(self, url):
        """hashes a URL: preferrably return an integer
        """
        raise NotImplementedError()


class MD5HashRing(HashRing):
    def __init__(self, cache_servers):
        self._max_digest_val = 2 ** 128 - 1
        self._hash_func = md5
        super().__init__(cache_servers)

    def _hash_url(self, url):
        m = self._hash_func()
        m.update(url.encode())
        return int(m.hexdigest(), 16) 

        
