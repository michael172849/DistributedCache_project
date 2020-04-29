import logging
import os
import sys

import grpc

sys.path.append(os.path.abspath("."))
from grpc_services import membership_pb2, membership_pb2_grpc
from grpc_services import content_service_pb2_grpc
from grpc_services import payload_pb2
import constant

MY_SERVER_ID = 7

def simple_send_heartbeat():
    with grpc.insecure_channel(constant.PROJECT_DOMAIN + constant.HEARTBEAT_PORT) as channel:
        stub = membership_pb2_grpc.MembershipManagementStub(channel)
        print("-------------- Send HeartBeat --------------")
        heartBeat = membership_pb2.HeartBeat(address="123", port="123")
        resMsg = stub.SendHeartBeat(heartBeat)
        print(resMsg)


def set_content(key, value):
    with grpc.insecure_channel(constant.PROJECT_DOMAIN + constant.CONTENT_SERVER_PORT) as channel:
        stub = content_service_pb2_grpc.ContentServiceStub(channel)
        print("-------------- Set Content --------------")
        request = payload_pb2.Request(client_id=MY_SERVER_ID, request_url=key, data=value)
        resMsg = stub.setContent(iter([request]))
        print(resMsg)

def get_content(key):
    with grpc.insecure_channel(constant.PROJECT_DOMAIN + constant.CONTENT_SERVER_PORT) as channel:
        stub = content_service_pb2_grpc.ContentServiceStub(channel)
        print("-------------- Get Content --------------")
        request = payload_pb2.Request(client_id=MY_SERVER_ID, request_url=key)
        resMsg = stub.getContent(request)
        print(resMsg)
        return resMsg.data

def startCacheServer():
    # simple_send_heartbeat()
    set_content('test','??!!!')
    print(get_content('test'))


if __name__ == '__main__':
    logging.basicConfig()
    startCacheServer()
