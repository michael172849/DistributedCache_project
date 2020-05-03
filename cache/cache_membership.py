import grpc
import constant
import os
import time
import threading
import psutil
import logging
from grpc_services import membership_pb2, membership_pb2_grpc
from grpc_services import content_service_pb2_grpc

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class HeartBeatThread(threading.Thread):
    def __init__(self, cache_server_id):
        threading.Thread.__init__(self)
        self.cache_server_id = cache_server_id
    
    def run(self):
        with grpc.insecure_channel(constant.PROJECT_DOMAIN + constant.HEARTBEAT_PORT) as channel:
            stub = membership_pb2_grpc.MembershipManagementStub(channel)
            while 1:
                logger.debug("-------------- Send HeartBeat --------------")
                pid = os.getpid()
                py = psutil.Process(pid)
                memoryUse = py.memory_info()# memory use in MB...I think
                cpuUse = py.cpu_percent()
                heartBeat = membership_pb2.HeartBeat(cacheServerId=str(self.cache_server_id), memoryUsage=str(memoryUse), cpuUsage=str(cpuUse))
                resMsg = stub.SendHeartBeat(heartBeat)
                logger.debug('-------------- Receive: {0} ------------'.format(resMsg))
                time.sleep(5)

class CacheMembership():
    def __init__(self, cache_server_id):
        super().__init__()
        self.cache_server_id = cache_server_id

    def start_membership_thread(self):
            try:
                heart_beat_thread = HeartBeatThread(self.cache_server_id)
                heart_beat_thread.start()
            except Exception as e:
                logger.error('exception: ', e)
