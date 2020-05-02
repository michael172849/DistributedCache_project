import logging
import os
import sys
from concurrent import futures

import grpc

sys.path.append(os.path.abspath("."))
from grpc_services import cache_service_pb2_grpc
from grpc_services import content_service_pb2_grpc
from grpc_services import payload_pb2

import constant

class ContentProxy():
    def __init__(self, server_id):
        super().__init__()
        self.serverId = server_id
        self.contentChannel = grpc.insecure_channel(constant.PROJECT_DOMAIN + constant.CONTENT_SERVER_PORT)
        self.contentServer = content_service_pb2_grpc.ContentServiceStub(self.contentChannel)

    def invalidate(self, key, cache_server_id):
        request = payload_pb2.Request(client_id = self.serverId,
                                    request_url = key)
        with grpc.insecure_channel(constant.PROJECT_DOMAIN + 
            str(constant.CACHE_SERVICE_PORT_START + cache_server_id)) as channel:
            stub = cache_service_pb2_grpc.CacheServiceStub(channel)
            resp = stub.invalidate(request)
            return resp.status        
        
    def setContent(self, key, value, cache_server_id):
        self.invalidate(key, cache_server_id)
        setRequest = payload_pb2.Request(client_id = self.serverId,
                                        request_url = key,
                                        data = value)
        resp = self.contentServer.setContent(setRequest)
        return resp.status

    def setCacheContent(self, key, value, cache_server_id):
        request = payload_pb2.Request(client_id = self.serverId,
                            request_url = key, data = value)
        with grpc.insecure_channel(constant.PROJECT_DOMAIN + 
            str(constant.CACHE_SERVICE_PORT_START + cache_server_id)) as channel:
            stub = cache_service_pb2_grpc.CacheServiceStub(channel)
            resp = stub.setContent(request)
            return resp.status

    def getContent(self, key, cache_server_id):
        getRequest = payload_pb2.Request(client_id = self.serverId,
                                        request_url = key)
        resp = None
        with grpc.insecure_channel(constant.PROJECT_DOMAIN + 
            str(constant.CACHE_SERVICE_PORT_START + cache_server_id)) as channel:
            stub = cache_service_pb2_grpc.CacheServiceStub(channel)
            resp = stub.getContent(getRequest)

        if resp.status == payload_pb2.Response.StatusCode.CACHE_HIT:
            return resp.status, resp.data

        if resp.status == payload_pb2.Response.StatusCode.CACHE_MISS:
            content = self.contentServer.getContent(getRequest)
            if content.status == payload_pb2.Response.StatusCode.OK:
                self.setCacheContent(key, content.data, cache_server_id)
                return resp.status, content.data
            elif content.status == payload_pb2.Response.StatusCode.NO_SUCH_KEY_ERROR:
                return content.status

        return resp.status



    
def test():
    with grpc.insecure_channel(constant.PROJECT_DOMAIN + str(constant.CACHE_SERVICE_PORT_START + 3)) as channel:
        stub = cache_service_pb2_grpc.CacheServiceStub(channel)
        logging.debug("-------------- test cache server --------------")
        setRequest = payload_pb2.Request(client_id = 0,
                                        request_url = "test",
                                        data = "test data!!!")
        getRequest = payload_pb2.Request(client_id = 0,
                                        request_url = "test")
        
        logging.debug('Test no key error')
        resMsg = stub.getContent(getRequest)
        logging.debug(resMsg.status)
        
        logging.debug('Test set content')
        resMsg = stub.setContent(setRequest)
        logging.debug(resMsg.status)

        logging.debug('Test cache miss')
        resMsg = stub.getContent(getRequest)
        logging.debug(resMsg.data)


def testProxy():
    logging.debug("-------------- test cache server --------------")
    manager = ContentProxy(0)
    logging.debug('Test no key error')
    logging.debug(manager.getContent('test', 0))

    logging.debug('Test set content')
    logging.debug(manager.setContent('test', 'test data!!!!',0))

    logging.debug('Test cache miss')
    logging.debug(manager.getContent('test', 0))

    logging.debug('Test cache hit')
    logging.debug(manager.getContent('test', 0))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    testProxy()