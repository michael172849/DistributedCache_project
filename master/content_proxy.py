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

    def setContent(self, key, value, cache_server_id):
        
        setRequest = payload_pb2.Request(client_id = self.serverId,
                                        request_url = key,
                                        data = value)
        resp = self.contentServer.setContent(setRequest)
        return resp

    def getContent(self, key, cache_server_id):
        getRequest = payload_pb2.Request(client_id = self.serverId,
                                        request_url = key)
        with grpc.insecure_channel(constant.PROJECT_DOMAIN + 
            str(constant.CACHE_SERVICE_PORT_START + cache_server_id)) as channel:
            stub = cache_service_pb2_grpc.CacheServiceStub(channel)
            resp = stub.getContent(getRequest)
            return resp
    
def test():
    with grpc.insecure_channel(constant.PROJECT_DOMAIN + str(constant.CACHE_SERVICE_PORT_START + 3)) as channel:
        stub = cache_service_pb2_grpc.CacheServiceStub(channel)
        logging.debug("-------------- test cache server --------------")
        setRequest = payload_pb2.Request(client_id = SERVER_ID,
                                        request_url = "test",
                                        data = "test data!!!")
        getRequest = payload_pb2.Request(client_id = SERVER_ID,
                                        request_url = "test")
        
        logging.debug('Test no key error')
        resMsg = stub.getContent(getRequest)
        logging.debug(resMsg.status)
        
        logging.debug('Test set content')
        resMsg = stub.setContent(setRequest)
        logging.debug(resMsg.status)

        logging.debug('Test cache hit')
        resMsg = stub.getContent(getRequest)
        logging.debug(resMsg.data)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test()