import multiprocessing
import os
import sys
import time
sys.path.append(os.path.abspath("."))
import constant
from cache.cache_server import startCacheServer



class CacheServerDriver():
    def __init__(self):
        self.mServers = {}

    def startCacheServers(self, server_cnt):
        print('Start {0} cache servers'.format(server_cnt))
        for i in range(server_cnt):
            self.mServers[i] = multiprocessing.Process(
                target=startCacheServer,
                args=(i,),
                daemon=True
                )
        for p in self.mServers:
            self.mServers[p].start()

    def stopAllCacheServers(self):
        for p in self.mServers:
            self.mServers[p].terminate()
        time.sleep(0.5)
        exit()

    def startCacheServer(self, server_num):
        if server_num in self.mServers:
            print('Server {0} already started'.format(server_num))
            return
        self.mServers[server_num] = multiprocessing.Process(
            target=startCacheServer,
            args=(server_num,),
            daemon=True,
        )
        self.mServers[server_num].start()

    def stopCacheServer(self, server_num):
        if server_num not in self.mServers:
            print('Server {0} is not started'.format(server_num))
            return
        self.mServers[server_num].terminate()
        time.sleep(0.1)
        del self.mServers[server_num]


def cmdInterface():
    driver = CacheServerDriver()
    while True:
        cmd = input()
        if 'startAll' in cmd:
            num = cmd.split()[1]
            driver.startCacheServers(int(num))
        elif 'stopAll' in cmd:
            driver.stopAllCacheServers()
        elif 'startServer' in cmd:
            num = cmd.split()[1]
            driver.startCacheServer(int(num))
        elif 'stopServer' in cmd:
            num = cmd.split()[1]
            driver.stopCacheServer(int(num))
        else:
            print('No such command! Available commands are:')
            print('1. startAll x. \t \tstart x cache servers.')
            print('2. startServer x. \t start one cache server with id x.')
            print('3. stopAll. \t \t stop all running cache servers.')
            print('4. stopServer x. \t stop one cache server with id x.')
            print()

if __name__ == '__main__':
    cmdInterface()



