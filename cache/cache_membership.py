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
    def __init__(self, stub):
        self._stub = stub
    
    def run(self):
        while 1:
            logger.debug("-------------- Send HeartBeat --------------")
            pid = os.getpid()
            py = psutil.Process(pid)
            memoryUse = py.memory_info()# memory use in MB...I think
            cpuUse = py.cpu_percent()
            heartBeat = membership_pb2.HeartBeat(memoryUsage=str(memoryUse), cpuUsage=str(cpuUse))
            resMsg = self._stub.SendHeartBeat(heartBeat)
            logger.debug('-------------- Receive: {0} ------------'.format(resMsg))
            time.sleep(5)

class CacheMembership():
    def start_membership_thread(self):
        with grpc.insecure_channel(constant.PROJECT_DOMAIN + constant.HEARTBEAT_PORT) as channel:
            stub = membership_pb2_grpc.MembershipManagementStub(channel)
            try:
                heart_beat_thread = HeartBeatThread(stub)
                heart_beat_thread.run()
            except Exception as e:
                logger.error('exception: ', e)
