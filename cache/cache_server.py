import logging
import time
import os
import sys
from concurrent import futures

import grpc


sys.path.append(os.path.abspath("."))
from grpc_services import membership_pb2, membership_pb2_grpc
from grpc_services import payload_pb2
from grpc_services import cache_service_pb2_grpc
from content.content_manager import ContentManager
from cache.cache_lru import LRUCache

import constant
from cache.cache_membership import CacheMembership



# Look aside cache server
class LookasideCache(cache_service_pb2_grpc.CacheServiceServicer):
    def __init__(self, server_id):
        super().__init__()
        self.cache_membership_manager = CacheMembership(server_id)
        self.mContentManager = ContentManager()
        self.mCache = LRUCache(constant.CACHE_SIZE)
        self.serverId = server_id
        self.token_granted = {}

    def getContent(self, request, context):
        logging.debug("Cache Server get content for client {0} for url {1}".format (
            request.client_id, request.request_url))
        key = request.request_url
        value = self.mCache.get(key)
        response = None
        if (value == constant.NO_SUCH_KEY_ERROR):
            logging.debug("cache miss for key = {0}".format(key))
            # check if this key has been granted with token within the time threshold
            lease = -1
            if key not in self.token_granted.keys() or (int(time.time()) - self.token_granted[key] > constant.CACHE_TOKEN_RATE_LIMITER):
                lease = int(time.time())
                # store the lease
                self.token_granted[key] = lease

            response = payload_pb2.Response(
                status = payload_pb2.Response.StatusCode.CACHE_MISS,
                request_url = key,
                lease = lease,
            )
        else:
            logging.debug("cache hit for key = {0}".format(key))
            response = payload_pb2.Response(
                status = payload_pb2.Response.StatusCode.CACHE_HIT,
                request_url = key,
                data = value
            )
        return response

    def setContent(self, request, context):
        logging.debug("Cache Server set content for client ({0}) for url {1}, lease {2}".format (
            request.client_id, request.request_url, request.lease))
        key = request.request_url
        value = request.data
        if key in self.token_granted.keys() and self.token_granted[key] == request.lease:
            # this is valid
            self.mCache.put(key, value)
            self.token_granted.pop(key)
            response = payload_pb2.Response(
                status = payload_pb2.Response.StatusCode.OK,
                request_url = key,
            )
        else:
            # not valid token... return with failed
            response = payload_pb2.Response(
                status = payload_pb2.Response.StatusCode.FAILED,
                request_url = key,
            )
        return response

    def invalidate(self, request, context):
        key = request.request_url
        logging.debug("Cache Server invalidate for client ({0}) for url {1}".format (
            request.client_id, key))
        self.mCache.delete(key)
        #TODO: add cache invalidate

        # invalidate token
        try:
            self.token_granted.pop(key)
        except:
            logging.debug("Not found token")
        response = payload_pb2.Response(
            status = payload_pb2.Response.StatusCode.OK,
            request_url = key,
        )
        return response  

def startCacheServer(server_id):
    logging.basicConfig(filename='log/cache_log_{0}'.format(server_id), filemode='w', level=constant.PROJ_LOG_LEVEL)

    # start cache side membership manager thread to send heartbeat to master...
    cacheServer = LookasideCache(server_id)

    # start cache server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cache_service_pb2_grpc.add_CacheServiceServicer_to_server(cacheServer, server)

    address =constant.getCacheServerAddr(server_id) 
    server.add_insecure_port(address)
    
    logging.debug("-----------------Start Cache Server {0} on address {1}--------------".format(server_id, address))
    server.start()
    cacheServer.cache_membership_manager.start_membership_thread()
    server.wait_for_termination()



if __name__ == '__main__':
    startCacheServer(0)
