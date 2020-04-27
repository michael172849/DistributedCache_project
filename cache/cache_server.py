import sys
import os
sys.path.append(os.path.abspath("."))
from membership import membership_pb2_grpc
from membership import membership_pb2

import logging
import grpc

def simple_send_heartbeat(stub):
    heartBeat = membership_pb2.HeartBeat(address="123", port="123")
    resMsg = stub.SendHeartBeat(heartBeat)
    print(resMsg)

def startCacheServer():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = membership_pb2_grpc.MembershipManagementStub(channel)
        print("-------------- Send HeartBeat --------------")
        simple_send_heartbeat(stub)

if __name__ == '__main__':
    logging.basicConfig()
    startCacheServer()