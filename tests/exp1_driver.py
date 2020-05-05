import sys
import os
import requests
import logging
import string
import random


sys.path.append(os.path.abspath("."))
import constant

logging.basicConfig(level=constant.PROJ_LOG_LEVEL)
domain = "http://0.0.0.0:5000/"
content_url = domain + "kv"
analytics_url = domain + 'analytics'
log_filename = 'log/analytics.txt'
num_entries = 1000
str_len_a = 3
str_len_b = 20
random_data = {}

def sendPostRequests(key, value):
    data = {'key': key, 'value': value}
    logging.debug("POST {0}".format(str(data)))
    r = requests.post(content_url, data=data)
    logging.debug("re: {0}".format(r.text))

def sendGetRequest(key):
    payload = 'key=' + key
    logging.debug('GET {0}'.format(str(payload)))
    r = requests.get(content_url + '?' + payload)
    logging.debug('resp: {0}'.format(r.text))

def stepInc():
    logging.debug('step increase')
    r = requests.get(analytics_url + '?' + 'op=step')
    logging.debug('resp: {0}'.format(r.text))

def printAnalytics(file = False):
    logging.debug('print analytics')
    if file:
        r = requests.get(analytics_url + '?' + 'op=print&file=' + log_filename)
    else:
        r = requests.get(analytics_url + '?' + 'op=print')
    logging.debug('resp: {0}'.format(r.text))

def fetch_all():
    for k in random_data:
        sendGetRequest(k)

def generate_random_string(len):
    pool = string.ascii_letters + string.digits 
    return ''.join(random.choice(pool) for i in range(len))

def step1():
    for i in range(num_entries):
        key = generate_random_string(random.randint(str_len_a, str_len_b))
        value = generate_random_string(random.randint(str_len_a, str_len_b))
        random_data[key] = value
        sendPostRequests(key, value)

def step2():
    fetch_all()

def step3():
    fetch_all()

def step4():
    fetch_all()

def step5():
    fetch_all()

def step6():
    fetch_all()



def cmdInterface():
    curStep = 0
    exp_steps = [step1, step2, step3, step4, step5, step6]

    while True:
        cmd = input()
        if cmd == 's':
            exp_steps[curStep]()
            curStep += 1
            stepInc()
        elif cmd == 'p':
            printAnalytics(True)
        elif cmd == 'pc':
            printAnalytics()
        else:
            print('No such command! Available commands are:')
            print('1. s \t \tstep to next step.')
            print('2. p \t \tprint analytics to file.')
            print('3. pc \t \tprint analytics to console.')

            print()

if __name__ == '__main__':
    cmdInterface()