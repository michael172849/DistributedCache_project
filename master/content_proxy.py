import logging
import os, sys
from concurrent import futures
import time
import grpc
import threading

sys.path.append(os.path.abspath("."))
from grpc_services import cache_service_pb2_grpc
from grpc_services import content_service_pb2_grpc
from grpc_services import payload_pb2

from analytics.analyzer import Analyzer
import constant

class ContentProxy():
    def __init__(self, server_id):
        super().__init__()
        self.serverId = server_id
        self.contentChannel = grpc.insecure_channel(constant.PROJECT_DOMAIN + constant.CONTENT_SERVER_PORT)
        self.contentServer = content_service_pb2_grpc.ContentServiceStub(self.contentChannel)
        self.analyzer = Analyzer()
        self.analyzer.start()

    def invalidate(self, key, cache_server_id):
        request = payload_pb2.Request(client_id = self.serverId,
                                    request_url = key)
        with grpc.insecure_channel(constant.getCacheServerAddr(cache_server_id)) as channel:
            stub = cache_service_pb2_grpc.CacheServiceStub(channel)
            resp = stub.invalidate(request)
            return resp.status 
        
    def setContent(self, key, value, cache_server_id):
        logging.debug('send SetRequest to content server for the key {0}'.format(key))
        setRequest = payload_pb2.Request(client_id = self.serverId,
                                        request_url = key,
                                        data = value)
        try:
            resp = self.contentServer.setContent(setRequest)
        except:
            logging.error("not able to communicate to content server")
            return constant.CONNECTION_CONTENT_SERVER_FAILED

        logging.debug('invalidating the key {0}'.format(key))

        if cache_server_id == "ERROR":
            logging.error("No cache server found....., not invalidate the cache")
        else:
            logging.info("invalidate key {0} for cacheserver {1}".format(key, cache_server_id))
            self.invalidate(key, int(cache_server_id))
        return resp.status

    def setCacheContent(self, key, value, cache_server_id, lease):
        request = payload_pb2.Request(client_id = self.serverId,
                            request_url = key, data = value, lease = lease)
        logging.debug('set content to cache server {0} for key {1} value {2} lease {3}'.format(cache_server_id, key, value, lease))
        with grpc.insecure_channel(constant.getCacheServerAddr(cache_server_id)) as channel:
            stub = cache_service_pb2_grpc.CacheServiceStub(channel)
            resp = stub.setContent(request)
            return resp.status

    def getContentFromContentServer(self, getRequest):
        # ignore cache server, get the content directly from content server
        try:
            content = self.contentServer.getContent(getRequest)
        except:
            logging.error("not able to communicate to content server")
            return constant.CONNECTION_CONTENT_SERVER_FAILED, None

        if content.status == payload_pb2.Response.StatusCode.OK:
            logging.debug('getting content from content server for key {0}'.format(getRequest.request_url))
            return content.status, content.data
        elif content.status == payload_pb2.Response.StatusCode.NO_SUCH_KEY_ERROR:
            logging.debug('no such key from content server for key {0}'.format(getRequest.request_url))
            return content.status, None


    def getContent(self, key, cache_server_id):
        getRequest = payload_pb2.Request(client_id = self.serverId,
                                        request_url = key)
        resp = None
        # cannot find a corresponding cache server
        if cache_server_id == "ERROR":
            logging.error("No cache server available found for key {0}....., get data from content server directly".format(key))
            status, content = self.getContentFromContentServer(getRequest)
            if status == payload_pb2.Response.StatusCode.OK:
                return constant.NO_AVAILABLE_CACHE_SERVER, content
            else:
                return status, content

        cache_server_id = int(cache_server_id)
        logging.debug('getting key {0} from cache server {1}, server: {2}'.format(key, cache_server_id, constant.getCacheServerAddr(cache_server_id)))
        # if the fetch from cache server failed... go to content server directly
        try:
            with grpc.insecure_channel(constant.getCacheServerAddr(cache_server_id)) as channel:
                stub = cache_service_pb2_grpc.CacheServiceStub(channel)
                resp = stub.getContent(getRequest)
        except:
            logging.error("getting key {0} from cache server {1} failed.".format(key, cache_server_id))
            status, content = self.getContentFromContentServer(getRequest)
            if status == payload_pb2.Response.StatusCode.OK:
                return constant.CONNECTION_CACHE_SERVER_FAILED, content
            else:
                return status, content

        if resp.status == payload_pb2.Response.StatusCode.CACHE_HIT:
            logging.debug('getting cache hit for key {0}'.format(key))
            self.analyzer.addRecord(key, cache_server_id, True)
            return resp.status, resp.data

        if resp.status == payload_pb2.Response.StatusCode.CACHE_MISS:
            logging.debug('getting cache miss for key {0} fetching from content server'.format(key))
            self.analyzer.addRecord(key, cache_server_id, False)

            # check if it is granted with the lease
            lease = resp.lease
            if lease == -1:
                logging.debug("didn't get the lease, wait for a {0} seconds and retry".format(constant.MASTER_NO_LEASE_RETRY_TIME))
                # it does not get the lease, so wait for a few minutes and then resend the request to cache
                time.sleep(constant.MASTER_NO_LEASE_RETRY_TIME)
                return self.getContent(key, cache_server_id)
            
            # it has a lease, so let's get content with work
            status, data = self.getContentFromContentServer(getRequest)
            if status == payload_pb2.Response.StatusCode.OK:
                # start a new thread to set cache
                set_cache_thread = threading.Thread(target=self.setCacheContent, args=(key, data, cache_server_id, lease, ))
                set_cache_thread.start()
                return resp.status, data
            else: 
                return status
        return resp.status

    def printAnalytics(self, filename = None):
        if self.analyzer == None:
            return
        self.analyzer.printAnalytics(filename)

    
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