import requests
import time
import threading
import os
import sys
from loguru import logger
from collections import deque

from util.CookieManager import CookieManager
from util.KVDatabase import KVDatabase

class CppRequest:
    config_path = os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "config.json")
    configDB = KVDatabase(config_path)
    global_request_queue = deque() 
    global_queue_lock = threading.Lock() 
    global_condition = threading.Condition(global_queue_lock)  # 条件变量
    maxRetry = configDB.get("maxRetry") if configDB.contains("maxRetry") else 3
    maxRatePerMinute = configDB.get("maxRatePerMinute") if configDB.contains("maxRatePerMinute") else 60
    retryInterval = configDB.get("retryInterval") if configDB.contains("retryInterval") else 20
    maxWaitTime = configDB.get("maxWaitTime") if configDB.contains("maxWaitTime") else 10


    def __init__(self, 
                headers=None, 
                cookies_config_path=""):    
        self.session = requests.Session()
        self.cookieManager = CookieManager(cookies_config_path)
        self.headers = headers or {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5,ja;q=0.4',
            'cookie': "",
            'origin': 'https://cp.allcpp.cn',
            'priority': 'u=1, i',
            'referer': 'https://cp.allcpp.cn/',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Microsoft Edge";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
        }

    
    def _checkRequestRate(self):
        # check request rate, remove requests that are older than 60s
        with self.global_queue_lock:
            currentTime = time.time()
            # if maxRatePerMinute <= 0, rate controll disabled, no need to check
            if self.maxRatePerMinute <= 0:
                return
            while len(self.global_request_queue) > 0 and currentTime - self.global_request_queue[0] > 60:
                self.global_request_queue.popleft()
            
            # if requestQueue is full, all threads should wait
            if len(self.global_request_queue) >= self.maxRatePerMinute:
                waitime = 60 - (currentTime - self.global_request_queue[0])
                logger.warning(f"Request rate limit reached, waiting for {waitime} seconds")
                self.global_condition.wait(waitime)
                logger.warning("Request rate limit released")
            # add current request time to queue
            currentTime = time.time()
            self.global_request_queue.append(currentTime)
        return
    
    def _requestSingle(self, method, url, data=None, headers=None):
        self._checkRequestRate()
        if not headers:
            headers = self.headers.copy()
        headers["cookie"] = self.cookieManager.get_cookies_str()
        response = None
        if self.maxWaitTime <= 0:
            response = self.session.request(method, url, data=data, headers=headers)
        else:
            response = self.session.request(method, url, data=data, headers=headers, timeout=self.maxWaitTime)
        response.raise_for_status()
        return response
    
    def _requestWithRetry(self, method, url, data=None, headers=None):
        for i in range(self.maxRetry):
            try:
                return self._requestSingle(method, url, data, headers)
            except Exception as e:
                logger.error(f"Request failed for {e}, retrying {i+1}/{self.maxRetry}")
                if self.retryInterval > 0:
                    time.sleep(self.retryInterval)
        logger.error("Request failed after retry")
        return None
    
    def request(self, method, url, data=None, headers=None):
        if self.maxRetry <= 0:
            return self._requestSingle(method, url, data, headers)
        else:
            return self._requestWithRetry(method, url, data, headers)


    def get(self, url, data=None, headers=None):
        return self.request("GET", url, data, headers)

    def post(self, url, data=None, headers=None):
        return self.request("POST", url, data, headers)

    def get_request_name(self):
        try:
            if not self.cookieManager.have_cookies():
                return "未登录"
            result = self.get("https://www.allcpp.cn/allcpp/circle/getCircleMannage.do").json()
            return result["result"]["joinCircleList"][0]["nickname"]
        except Exception as e:
            return "未登录"

    def refreshToken(self):
        self.cookieManager.refreshToken()
    
    def getHeaders(self):
        return self.headers


if __name__ == "__main__":
    test_request = CppRequest(cookies_config_path="cookies.json")
    res = test_request.get("https://www.allcpp.cn/api/tk/getList.do?type=1&sort=0&index=1&size=10")
    print(res.headers)
    print(res.text)
