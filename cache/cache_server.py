import logging
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

SERVER_ID = 3

def simple_send_heartbeat():
    with grpc.insecure_channel(constant.PROJECT_DOMAIN + constant.HEARTBEAT_PORT) as channel:
        stub = membership_pb2_grpc.MembershipManagementStub(channel)
        print("-------------- Send HeartBeat --------------")
        heartBeat = membership_pb2.HeartBeat(address="123", port="123")
        resMsg = stub.SendHeartBeat(heartBeat)
        print(resMsg)

class CacheServer(cache_service_pb2_grpc.CacheServiceServicer):
    def __init__(self):
        super().__init__()
        self.mContentManager = ContentManager()
        self.mCache = LRUCache(constant.CACHE_SIZE)

    def getContent(self, request, context):
        logging.debug("Cache Server get content for client (%d) for url %s" % 
            (request.client_id, request.request_url))
        key = request.request_url
        value = self.mCache.get(key)
        response = None
        if (value == constant.NO_SUCH_KEY_ERROR):
            logging.debug("cache miss for key = %s" % key)
            response = self.mContentManager.getContent(key, SERVER_ID)
        else:
            logging.debug("cache hit for key = %s" % key)
            response = payload_pb2.Response(
                status = payload_pb2.Response.StatusCode.OK,
                request_url = key,
                data = value
            )
        return response

    def setContent(self, request, context):
        logging.debug("Cache Server set content for client (%d) for url %s" % 
            (request.client_id, request.request_url))
        key = request.request_url
        value = request.data
        self.mCache.put(key, value)
        resp = self.mContentManager.setContent(key, value, SERVER_ID)
        return resp


def startCacheServer():
    # simple_send_heartbeat()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cache_service_pb2_grpc.add_CacheServiceServicer_to_server(CacheServer(), server)

    server.add_insecure_port(constant.PROJECT_DOMAIN + (str)(constant.CACHE_SERVICE_PORT_START + SERVER_ID))
    
    logging.debug("-----------------Start Cache Server %d--------------" % (SERVER_ID))
    server.start()
    server.wait_for_termination()



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    startCacheServer()
