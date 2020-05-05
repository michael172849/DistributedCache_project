from bintrees import AVLTree
from hashlib import md5
from numpy import log2
import numpy as np
import random
import logging
import copy
# maps URI to the cache server id
# implements https://www.cs.princeton.edu/courses/archive/fall07/cos518/papers/chash.pdf
DEBUG = False
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
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
        logger.info("INIT HASHRING")
        self._shuffle_cache_servers = True
        # Tree that maps buckets to cache servers. Represents the whole hashring.
        self._val_to_serv_url = AVLTree()  

        if len(cache_servers) == 0:
            self._n = 0
        else:
            self._n = int(np.ceil(log2(len(cache_servers))))

        # divide the hash ring into 2^n intervals
        self._num_buckets = np.power(2, self._n)
        # make interval * (2 ^ n) is slightly bigger than the max_digest_val
        self._interval = int(np.ceil(self._max_digest_val / self._num_buckets))
        self._buckets = [i for i in range(0, self._max_digest_val, self._interval)]

        # one balancing binary tree is used per bucket to store urls. Represents single cache server 
        # an element in a tree maps hashed url to url itself
        self._cache_trees = dict([(serv, AVLTree()) for serv in cache_servers])

        # randomly allocate cache servers to buckets
        if len(cache_servers) != 0:
            self._add_servers_to_remaining_buckets(cache_servers)

        if DEBUG:
            print(self._val_to_serv_url)

    def _get_clockwise_value(self, tree, val):
        try:
            return tree.ceiling_item(val)[0]
        except:
            return tree.ceiling_item(-1)[0]
    
    def _get_clockwise_url(self, tree, val):
        try:
            return tree.ceiling_item(val)[1]
        except:
            return tree.ceiling_item(-1)[1]

    def _get_cache_server_url(self, url):
        return self._get_clockwise_url(self._val_to_serv_url, self._hash_url(url))

    def _get_cache_server(self, url):
        if len(self._val_to_serv_url) == 0:
            return 'ERROR'
        return self._get_clockwise_url(self._val_to_serv_url, self._hash_url(url))

    def _get_cache_server_value(self, val):
        return self._get_clockwise_value(self._val_to_serv_url, val)

    def _bisect_segments(self, num_new_buckets):
        """bisect the segments on the hashring so that total number of buckets will be 2^(n+1)
        """
        new_n = int(np.ceil(log2(len(self._val_to_serv_url) + num_new_buckets)))

        for n in range(self._n+1, new_n+1):
            new_interval = int(np.ceil(self._max_digest_val / np.power(2, n)))
            for i in range(new_interval, self._max_digest_val, self._interval):
                self._buckets.append(i)
            self._interval = new_interval
        
        self._n = new_n
        try:
            self._interval = new_interval
        except:
            print("exceeded capacity")

    def _add_servers_to_remaining_buckets(self, cache_servers):
        """this function allocates cache servers to remaining buckets as many as it can
        """
        if len(self._buckets) == 0:
            return cache_servers
        random.shuffle(self._buckets)
        new_mappings = AVLTree()
        
        alloc_len = min(len(self._buckets), len(cache_servers))
        buckets_to_be_allocated = self._buckets[:alloc_len]

        if not self._shuffle_cache_servers:
            buckets_to_be_allocated.sort()

        for i in range(alloc_len):
            new_mappings.insert(buckets_to_be_allocated[i], cache_servers[i])
            self._cache_trees[cache_servers[i]] = AVLTree()

        rem_buckets = self._buckets[alloc_len:]
        rem_cache_servers = cache_servers[alloc_len:]

        for old_bucket_val in self._val_to_serv_url.keys():
            try:
                old_cache_server = self._cache_trees[self._val_to_serv_url[old_bucket_val]]
                old_cache_server_copy = copy.deepcopy(self._cache_trees[self._val_to_serv_url[old_bucket_val]])
            except KeyError:
                continue
            for hashed_url in old_cache_server_copy.keys():
                new_bucket_val = self._get_clockwise_value(new_mappings, hashed_url)
                
                clockwise_val_to_new_bucket = new_bucket_val - int(hashed_url)
                if clockwise_val_to_new_bucket < 0:
                    clockwise_val_to_new_bucket += self._max_digest_val + 1

                clockwise_val_to_old_bucket = old_bucket_val - int(hashed_url)
                if clockwise_val_to_old_bucket < 0:
                    clockwise_val_to_old_bucket += self._max_digest_val + 1

                # print("for item " + str(hashed_url))
                # print("old server val " + str(old_bucket_val))
                # print("new server val " + str(new_bucket_val))
                # print("old: {} vs. new: {}".format(clockwise_val_to_old_bucket, clockwise_val_to_new_bucket))

                if clockwise_val_to_new_bucket < clockwise_val_to_old_bucket:
                    # print(str(clockwise_val_to_new_bucket) + ":migrating " + old_cache_server[hashed_url])
                    # @TODO callback to send cache to new server
                    self._cache_trees[new_mappings[new_bucket_val]].insert(hashed_url, old_cache_server[hashed_url])
                    # @TODO callback to invalidate old cache here
                    old_cache_server.remove(hashed_url)

        for k in new_mappings.keys():
            self._val_to_serv_url.insert(k, new_mappings[k])

        self._buckets = rem_buckets
        return rem_cache_servers

    def shuffle_cache_servers(self):
        self._shuffle_cache_servers = True
    
    def no_shuffle_cache_servers(self):
        self._shuffle_cache_servers = False

    def get_cache_server_with_check(self, url):
        """returns a cache server in which the cache is stored
        raises KeyError if url is not found @TODO how to handle this?
        """
        server_url = self._get_cache_server_url(url)
        self._cache_trees[server_url][self._hash_url(url)]
        return server_url

    def add_cache(self, url):
        """add a cache
        """
        # @TODO what if add an existing cache
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

    def add_single_cache_server(self, cache_server):
        self.add_cache_servers([cache_server])

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
                raise Exception("Servers not allocated: " + ' '.join(rem_cache_servers))

    def remove_cache_server(self, server_url):
        logger.debug("removed cache_server {0}".format(server_url))
        if not self._val_to_serv_url:
            return
        items = (copy.deepcopy(self._val_to_serv_url)).items()
        for val, url in items:
            if url == server_url:
                del self._val_to_serv_url[val]
                self._buckets.append(val)
                # @TODO callback to invalidate all the cache
                old_cache_server = self._cache_trees[server_url]
                try:
                    new_cache_server_url = self._get_clockwise_cache_server_value(val+1)
                    self._cache_trees[self._val_to_serv_url[old_cache_server]].update(old_cache_server)
                except:
                    pass
                # @TODO callback to send the cache to new cache server
        del self._cache_trees[server_url]

    
    def _hash_url(self, url):
        """hashes a URL: preferrably return an integer
        """
        raise NotImplementedError()

class SimpleHashRing(HashRing):
    def __init__(self, cache_servers):
        self._max_digest_val = 2 ** 5 - 1
        self._hash_func = md5
        super().__init__(cache_servers)

    def _hash_url(self, url):
        m = self._hash_func()
        m.update(url.encode())
        return int(m.hexdigest(), 16) % self._max_digest_val

class MD5HashRing(HashRing):
    def __init__(self, cache_servers):
        self._max_digest_val = 2 ** 128 - 1
        self._hash_func = md5
        super().__init__(cache_servers)

    def _hash_url(self, url):
        m = self._hash_func()
        m.update(url.encode())
        return int(m.hexdigest(), 16) 

class DuplicateMD5HashRing(MD5HashRing):
    def __init__(self, cache_servers, dup_num):
        super().__init__(cache_servers * dup_num)
        self.dup_num = dup_num

    def add_cache_servers(self, cache_servers):
        super().add_cache_servers(cache_servers * self.dup_num)


