import multiprocessing
import os
import sys
import time
sys.path.append(os.path.abspath("."))
import constant
from cache.cache_server import startCacheServer


m_servers = []
def startCacheServers(server_num):
    for i in range(server_num):
        m_servers.append(multiprocessing.Process(
            target=startCacheServer,
            args=(i,),
            daemon=True
            ))
    for p in m_servers:
        p.start()

def stopCacheServers():
    for p in m_servers:
        p.terminate()
    time.sleep(0.5)
    exit()

if __name__ == '__main__':
    while True:
        cmd = input()
        if 'start ' in cmd:
            num = cmd.split()[1]
            startCacheServers(int(num))
        elif cmd == 'stop':
            stopCacheServers()


