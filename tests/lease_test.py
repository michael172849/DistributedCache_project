import requests
import unittest
import threading
import logging, sys, time
unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: x < y

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

class TestLease(unittest.TestCase):
    def setUp(self):
        self.url = "http://0.0.0.0:5000/"
        self.kv_url = self.url + "kv"

    def test_1_lease_thundering_herd(self):
        """one of the get thread should be cache miss, and all others be cache hit
            This also reduces content server load
        """
        logger.info("--------------------------Test thundering herd ----------------------------------------")
        data = {'key': 'test_lease_1', 'value': 'hello CS380'}
        r = requests.post(self.kv_url, data=data)
        payload1 = {'key':'test_lease_1'}
        thread1 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])
        thread2 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])
        thread3 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])
        thread4 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])
        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()

        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()
        logger.info("---------------------------Finished----------------------------------")

    def test_2_lease_latency(self):
        logger.info("---------------------------Test Lease latency ----------------------------------------")
        data = {'key': 'test_lease_2', 'value': 'Nice job!!!'}
        r = requests.post(self.kv_url, data=data)
        payload1 = {'key':'test_lease_2'}
        thread1 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])
        thread2 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])
        thread3 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])
        thread4 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])
        thread1.start()

        time.sleep(2)
        thread2.start()
        time.sleep(1)
        thread3.start()
        thread4.start()

        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()
        logger.info("---------------------------Finished----------------------------------")

    def test_3_lease_stale_data(self):
        logger.info("---------------------------Test stale data ----------------------------------------")
        data = {'key': 'test_lease_3', 'value': 'OH Before update!!!'}

        logger.info("send first post request {0}".format(data))
        r = requests.post(self.kv_url, data=data)
        payload1 = {'key':'test_lease_3'}
        thread1 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])

        data2 = {'key': 'test_lease_3', 'value': 'after update!!!'}
        thread2 = threading.Thread(target=send_post_thread, args=[self.kv_url, data2])
                
        thread3 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])
        thread4 = threading.Thread(target=send_get_thread, args=[self.kv_url, payload1])

        thread1.start()
        time.sleep(1)
        thread2.start()
        thread3.start()

        thread1.join()
        thread2.join()
        thread3.join()
        logger.info("---------------------------Finished----------------------------------")

def send_get_thread(url, payload):
    start_time = time.time()
    logger.info("Thread {0} send get request".format(threading.current_thread().getName()))
    r = requests.get(url, params=payload)
    time_spend = time.time() - start_time
    logger.info("Thread {0} time spent: {2} sec. Get response: {1}".format(threading.current_thread().getName(), r.text, time_spend))

def send_post_thread(url, data):
    start_time = time.time()
    logger.info("Thread {0} send post request".format(threading.current_thread().getName()))
    r = requests.post(url, data=data)
    time_spend = time.time() - start_time
    logger.info("Thread {0} time spent: {2} sec. Get response: {1}".format(threading.current_thread().getName(), r.text, time_spend))

if __name__ == '__main__':
    unittest.main()