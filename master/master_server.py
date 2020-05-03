from aiohttp import web
import logging
import json
import time
import sys, os
sys.path.append(os.path.abspath("."))
import constant
from master.membership_manager import MembershipManager
from master.content_proxy import ContentProxy
from master.hashring import SimpleHashRing

class MasterServer():
    def __init__(self):
        self.membership_manager = MembershipManager(self.add_cache_server, self.rm_cache_server)
        self.content_proxy = ContentProxy(constant.MASTER_SERVER_ID)
        self.hashring = SimpleHashRing(self.membership_manager.get_cache_server_list())

    def add_cache_server(self, server_url):
        self.hashring.add_single_cache_server(server_url)

    def rm_cache_server(self, server_url):
        self.hashring.remove_cache_server(server_url)

    def start_server(self):
        self.membership_manager.start_membership_service()

    def set_content(self, key, value):
        # get cache server id based on key
        cache_server_id = self.hashring._get_clockwise_cache_server(key)
        print("type: {0}, val: {1}".format(type(cache_server_id), cache_server_id))
        logging.info("cache server id: {0} for key {1}".format(cache_server_id, key))
        logging.info(self.hashring._val_to_serv_url)
        self.content_proxy.setContent(key, value, int(cache_server_id))

    def get_content(self, key):
        cache_server_id = self.hashring._get_clockwise_cache_server(key)
        return self.content_proxy.getContent(key, int(cache_server_id))

MASTERSERVER = MasterServer()

logging.basicConfig(level=logging.DEBUG)

async def handle(request):
    response_obj = {'status': 'success'}
    return web.Response(text=json.dumps(response_obj))

async def set_value(request):
    data = await request.post()
    try:
        key = data['key']
        value = data['value']
    except KeyError:
        logging.error("missing key, value for POST")
        response_obj = {'status': 'error'}
        return web.Response(text=json.dumps(response_obj)) 

    re = MASTERSERVER.set_content(key, value)
    response_obj = {'status': re}
    return web.Response(text=json.dumps(response_obj)) 

async def get_value(request):
    key = request.query['key']
    re = MASTERSERVER.get_content(key)
    response_obj = {'status': re}
    return web.Response(text=json.dumps(response_obj)) 

def run():
    MASTERSERVER.start_server()
    app = web.Application()
    app.router.add_get('/', handle)
    app.router.add_post('/kv', set_value)
    app.router.add_get('/kv', get_value)
    web.run_app(app)

if __name__ == "__main__":
    run() 