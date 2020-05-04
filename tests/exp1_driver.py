import sys
import os
import requests
import logging
sys.path.append(os.path.abspath("."))
from tests import setup


logging.basicConfig(level=logging.DEBUG)
domain = "http://0.0.0.0:5000/"
content_url = domain + "kv"
random_data = {}
exp_steps = [step2]

def sendPostRequests(key, value):
    data = {'key': key, 'value': value}
    logging.debug("POST {0}".format(str(data)))
    r = requests.post(content_url, data=data)
    logging.info("re: {0}".format(r.text))

def sendGetRequest(key):
    payload = 'key=' + key
    logging.debug('GET {0}'.format(str(payload)))
    r = requests.get(content_url + '?' + payload)
    logging.info('resp: {0}'.format(r.text))


def step2():
    for k in random_data:
        sendPostRequests(k)

def step3():
    pass

def step4():
    pass

def cmdInterface():
    curStep = 0
    while True:
        cmd = input()
        if cmd == 'step':
            exp_steps[curStep]()
            curStep += 1
        else:
            print('No such command! Available commands are:')
            print('1. s \t \tstep to next step.')
            print()

if __name__ == '__main__':
    cmdInterface()