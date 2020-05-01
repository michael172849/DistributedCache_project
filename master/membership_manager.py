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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class MembershipService(membership_pb2_grpc.MembershipManagementServicer):
    def __init__(self):
        super().__init__()
        self.cache_servers = {}

    def SendHeartBeat(self, request, context):
        """Send heartbeat
        A client to server function call
        """
        self.cache_servers[context.peer()] = {
            "status": request,
            "time":   time.time(),
        }
        logger.debug(self.cache_servers)
        return membership_pb2.ResultMsg(status=membership_pb2.ResultMsg.StatusCode.SUCCEEDED, msg="heartbeat received")

class MembershipManager():
    def __init__(self):
        self._membership_service = MembershipService()
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  

    def start_membership_service(self):
        logger.debug("membership service start")
        membership_pb2_grpc.add_MembershipManagementServicer_to_server(
            self._membership_service, self.server)
        self.server.add_insecure_port(constant.PROJECT_DOMAIN + constant.HEARTBEAT_PORT)
        self.server.start()

    def get_cache_server_list(self):
        return list(self._membership_service.cache_servers.keys())

    def get_cache_server_size(self):
        return len(self._membership_service.cache_servers)
        
