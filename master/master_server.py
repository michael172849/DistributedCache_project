import flask
import logging
import time
import sys, os
sys.path.append(os.path.abspath("."))
import constant
from master.membership_manager import MembershipManager
from master.content_proxy import ContentProxy
from master.hashring import SimpleHashRing

class MasterServer():
    def __init__(self):
        app.debug = True
        self.membership_manager = MembershipManager(self.add_cache_server, self.rm_cache_server)
        self.content_proxy = ContentProxy(constant.MASTER_SERVER_ID)
        self.hashring = SimpleHashRing(self.membership_manager.get_cache_server_list())

    def add_cache_server(self, server_url):
        self.hashring.add_single_cache_server(server_url)

    def rm_cache_server(self, server_url):
        self.hashring.remove_cache_server(server_url)

    def start_server(self):
        self.membership_manager.start_membership_service()
        app.run()

    def set_content(self, key, value):
        # get cache server id based on key
        cache_server_id = self.hashring._get_clockwise_cache_server(key)
        logging.debug("cache server id: {0} for key {1}".format(cache_server_id, key))
        self.content_proxy.setContent(key, value, cache_server_id)

    def get_content(self, key):
        cache_server_id = self.hashring._get_clockwise_cache_server(key)
        return self.content_proxy.getContent(key, cache_server_id)



app = flask.Flask(__name__)
master_server = MasterServer()

logging.basicConfig(level=logging.DEBUG)



@app.route("/")
def hello():
    return "Hello World!"

@app.route("/kv", methods=['GET', 'POST'])
def getKeyValue():
    d = []
    if flask.request.method == 'POST':
        key = flask.request.values.get('key')
        value = flask.request.values.get('value')
        logging.info("Get POST request {0}, {1}".format(key, value))
        master_server.set_content(key, value)
    
    if flask.request.method == 'GET':
        key = flask.request.values.get('key')
        d['data'] = master_server.get_content(key)
        return flask.jsonify(d)




if __name__ == "__main__":
    master_server.start_server()