import logging
import threading
import time
import os
import sys
from concurrent import futures

import grpc

sys.path.append(os.path.abspath("."))
from grpc_services import membership_pb2, membership_pb2_grpc
import constant
from master.hashring import SimpleHashRing

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class MembershipService(membership_pb2_grpc.MembershipManagementServicer):
    def __init__(self, add_cache_server_call_back):
        super().__init__()
        self.cache_servers = {}
        self._add_cache_server_call_back = add_cache_server_call_back

    def is_server_validate(self, key):
        if key not in self.cache_servers.keys():
            return False
        
        if time.time() - self.cache_servers[key]["time"] > constant.HEARTBEAT_TIMEOUT:
            # delete the cache server from the list
            return False
        return True

    def SendHeartBeat(self, request, context):
        """Send heartbeat
        A client to server function call
        """
        if request.cacheServerId not in self.cache_servers.keys():
            logger.info("Add cache server {0}: {1}".format(request.cacheServerId, context.peer()))
            self._add_cache_server_call_back(request.cacheServerId)

        self.cache_servers[request.cacheServerId] = {
            "context": context.peer(),
            "status": request,
            "time":   time.time(),
        }
        #logger.debug(self.cache_servers)
        return membership_pb2.ResultMsg(status=membership_pb2.ResultMsg.StatusCode.SUCCEEDED, msg="heartbeat received")

class MembershipMonitor(threading.Thread):
    def __init__(self, membership_service, rm_cache_server):
        threading.Thread.__init__(self)
        self._membership_service = membership_service
        self._rm_cache_server = rm_cache_server

    def run(self):
        while 1:
            # check membership every 10 seconds
            rm_list = []
            for key in self._membership_service.cache_servers:
                if not self._membership_service.is_server_validate(key):
                    logger.info("Remove cache server {0}".format(key))
                    self._rm_cache_server(key)
                    rm_list.append(key)
            for key in rm_list:
                self._membership_service.cache_servers.pop(key)
            time.sleep(10)


class MembershipManager():
    def __init__(self, add_cache_server, rm_cache_server):
        self._membership_service = MembershipService(add_cache_server)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10)) 
        self._membership_monitor = MembershipMonitor(self._membership_service, rm_cache_server)

    def start_membership_service(self):
        logger.info("membership service start")
        membership_pb2_grpc.add_MembershipManagementServicer_to_server(
            self._membership_service, self.server)
        self.server.add_insecure_port(constant.PROJECT_DOMAIN + constant.HEARTBEAT_PORT)
        self.server.start()

        ## start another thread to monitor old cache server
        self._membership_monitor.start()

    def get_cache_server_list(self):
        return list(self._membership_service.cache_servers.keys())

    def get_cache_server_size(self):
        return len(self._membership_service.cache_servers)
        
