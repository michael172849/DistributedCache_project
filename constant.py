import os
import sys

sys.path.append(os.path.abspath("."))
from grpc_services import payload_pb2

PROJECT_DOMAIN = '[::]:'
CONTENT_SERVER_PORT = '50052'
CONTENT_SERVER_ID = 1

HEARTBEAT_PORT = '50051'

CACHE_SERVICE_PORT_START = 50055

NO_SUCH_KEY_ERROR = -1

CACHE_SIZE = 100
CACHE_SERVER_COUNT = 3

STATUS_CODE = payload_pb2.Response.StatusCode

def getCacheServerAddr(cache_server_id):
    return PROJECT_DOMAIN + str(CACHE_SERVICE_PORT_START + cache_server_id)


