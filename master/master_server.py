import flask
import asyncio
import logging
import json
import time
import sys, os
sys.path.append(os.path.abspath("."))
import constant
from master.membership_manager import MembershipManager
from master.content_proxy import ContentProxy
from master.hashring import SimpleHashRing
from master.inconsistent_hash import InconsistentMd5Hash

class MasterServer():
    def __init__(self):
        self.membership_manager = MembershipManager(self.add_cache_server, self.rm_cache_server)
        self.content_proxy = ContentProxy(constant.MASTER_SERVER_ID)
        # self.hashring = SimpleHashRing(self.membership_manager.get_cache_server_list())
        self.hashring = InconsistentMd5Hash(self.membership_manager.get_cache_server_list())

    def add_cache_server(self, server_url):
        self.hashring.add_single_cache_server(server_url)

    def rm_cache_server(self, server_url):
        self.hashring.remove_cache_server(server_url)

    def start_server(self):
        self.membership_manager.start_membership_service()

    def set_content(self, key, value):
        # get cache server id based on key
        cache_server_id = self.hashring._get_cache_server(key)
        logging.debug("cache server id: {0} for key {1}".format(cache_server_id, key))
        return self.content_proxy.setContent(key, value, cache_server_id)

    def get_content(self, key):
        cache_server_id = self.hashring._get_cache_server(key)
        return self.content_proxy.getContent(key, cache_server_id)

    def printAnalytics(self, filename = None):
        self.content_proxy.printAnalytics(filename)

    def stepAnalytics(self):
        self.content_proxy.stepAnalytics()

MASTERSERVER = MasterServer()
app = flask.Flask(__name__)


logging.basicConfig(
    format="%(asctime)s - %(filename)s:%(funcName)s:%(lineno)d(%(levelname)s) - %(message)s",
    level=constant.PROJ_LOG_LEVEL,
    datefmt='%Y-%m-%d %H:%M:%S')



@app.route("/analytics", methods=['GET'])
def analyze():
    response_obj = {'status': 'success'}
    op = flask.request.args.get('op','')
    filename = flask.request.args.get('file', None)
    if op == 'print':
        MASTERSERVER.printAnalytics(filename)
    elif op == 'step':
        MASTERSERVER.stepAnalytics()
    return response_obj

@app.route("/kv", methods=["POST"])
def set_value():
    try:
        key = flask.request.values.get('key')
        value = flask.request.values.get('value')
    except:
        logging.error("missing key, value for POST")
        response_obj = {'status': 'error'}
        return response_obj

    re = MASTERSERVER.set_content(key, value)
    response_obj = {'status': constant.STATUS_MAP[re]}
    return response_obj

@app.route("/kv", methods=["GET"])
def get_value():
    key = flask.request.args.get('key')
    status, data = MASTERSERVER.get_content(key)
    response_obj = {'status': constant.STATUS_MAP[status], 'data': data}
    return response_obj

@app.route("/slow")
def slow():
    logging.info("get slow request")
    do_stuff()
    response_obj = {'status': 'success'}
    return response_obj

def do_stuff():
    time.sleep(5)
    return 1

@app.route('/')
def hello_world():
    return 'Hello, World!'

if __name__ == "__main__":
    MASTERSERVER.start_server()
    app.run()