from loguru import logger
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest
import os
import sys
import re
import json

import threading
from collections import deque
import concurrent.futures 
class cppEventCrawer:
    def __init__(self, eventID = -1, URL = "", maxWorker = 10):
        # read cookie config
        config_path = os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "config.json")
        configDB = KVDatabase(config_path)

        if not configDB.contains("cookie_path"):
            configDB.insert("cookie_path", os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "cookies.json"))
        
        cookie_path = configDB.get("cookie_path")

        # load cookie with CppRequest
        self.main_request = CppRequest(cookies_config_path=cookie_path)
        self.global_cookieManager = self.main_request.cookieManager

        # generate URL and load page
        if eventID == -1 and URL == "":
            logger.error("circleID and URL cannot be empty at the same time")
            exit(1)
        elif eventID == -1:
            eventID = self.extractEventID(URL)
            if eventID == None:
                logger.error("Bad URL")
                exit(1)

        self.eventID = eventID
        self.eventApi = "https://www.allcpp.cn/allcpp/event/getevents.do"

        # get event status from api
        headers = self.main_request.getHeaders()
        headers["content-type"] = "application/json"
        response = self.main_request.post(self.eventApi, data=json.dumps({"id": eventID}), headers=headers)
        if response.status_code != 200:
            logger.error("api request failed")
            return
        data = json.loads(response.text)
        if not data["isSuccess"]:
            logger.error("bad response")
            return
        self.data = data["result"]

        logger.info("Succeeded loading eventID" + str(eventID))
        # get dataIDs for products
        self.data_ids = self._getDataIDs()
        logger.info(f"Found {len(self.data_ids)} DataIDs: {self.data_ids}")
        
        self.maxWorker = maxWorker
        self.lock = threading.Lock()
        return

               
    def _getDataIDs(self):
        # get dataIDs for products
        if self.data == None:
            logger.error("No data available")
            return
        return [data["id"] for data in self.data]

    def getEventID(self):
        # return eventID
        return self.eventID
    
    def getInfos(self):
        if self.data == None:
            logger.error("No data available")
            return
        for data in self.data:
            yield data
    
    def getCircles(self, limitation=-1):
        # get circles with api, limitation is the maximum number of circles to return
        if self.data_ids == []:
            logger.error("No available DataIDs, getCircles failed")
            return
        num = 0
        fetchFlag = True
        for data_id in self.data_ids:
            if not fetchFlag:
                break
            emptyFlag = False
            pageIndex = 1

            while fetchFlag and not emptyFlag:
                circle_api = "https://www.allcpp.cn/api/circle/getcirclelist.do"
                request_data = {'eventid': data_id,
                        'search': '',
                        'orderbyid': 0,
                        'typeid': '',
                        'pageindex': pageIndex,
                        'pagesize': 50}
                headers = self.main_request.getHeaders()
                headers["content-type"] = "application/x-www-form-urlencoded; charset=UTF-8"
                response = self.main_request.post(url=circle_api, data=request_data, headers=headers)
                if response.status_code != 200:
                    logger.error("api request failed")
                    return []

                data = json.loads(response.text)
                if not data["isSuccess"]:
                    logger.error("bad response")
                    return []

                # get list on current page
                pageList = data["result"]
                emptyFlag = len(pageList) == 0
                logger.info(f"Getting Page {pageIndex}, {len(pageList)} circles from EventID{data_id}")
                for circle in pageList:
                    circle["eventId"] = self.eventID
                    circle["dataId"] = data_id
                    yield circle
                    num += 1
                    if limitation != -1 and num >= limitation:
                        fetchFlag = False
                        break
                pageIndex += 1
        logger.info(f"Finished getting {num} circles from EventID{data_id}")
                


    
    def getProducts(self, limitation=-1):
        # get products with api, limitation is the maximum number of products to return
        if self.data_ids == []:
            logger.warning("No available DataIDs, getProducts failed")
            return
        num = 0
        fetchFlag = True
        for data_id in self.data_ids:
            if not fetchFlag:
                break
            EmptyFlag = False
            pageIndex = 1

            # for each thread, fetch a page
            def fetchPage(pageIndex):
                nonlocal fetchFlag, num
                if not fetchFlag and not EmptyFlag:
                    return []
                dojin_api = "https://www.allcpp.cn/allcpp/event/getDoujinshiList.do"
                data = {
                    "eventid": data_id,
                    "searchstring": "",
                    "orderbyid": 3,
                    "typeid": "",
                    "pageindex": pageIndex,
                    "pagesize": 1000,
                    "sellstatus": "",
                    "sectionid": ""
                }
                headers = self.main_request.getHeaders()
                headers["content-type"] = "application/x-www-form-urlencoded; charset=UTF-8"
                response = self.main_request.post(dojin_api, data=data, headers=headers)
                if response.status_code != 200:
                    with self.lock:
                        logger.error("api request failed")
                    return []

                data = json.loads(response.text)
                if not data["isSuccess"]:
                    with self.lock:
                        logger.error("bad response")
                    return []

                # get list on current page
                pageList = data["result"]["list"]
                with self.lock:
                    logger.info(f"Getting Page {pageIndex}, {len(pageList)} products from EventID{data_id}")
                for product in pageList:
                    product["eventId"] = self.eventID
                    product["dataId"] = data_id
                return pageList
            
            # fetch pages with ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.maxWorker) as executor:
                futureToPageIndex = {executor.submit(fetchPage, pageIndex): pageIndex}
                while futureToPageIndex and fetchFlag and not EmptyFlag:
                    for future in concurrent.futures.as_completed(futureToPageIndex):
                        pageIndex = futureToPageIndex.pop(future)
                        try:
                            product = future.result()
                            if product == [] or product == None:
                                EmptyFlag = True
                                break
                            for i in product:
                                yield i
                                num += 1
                                if limitation != -1 and num >= limitation:
                                    fetchFlag = False
                                    break
                        except Exception as e:
                            with self.lock:
                                logger.error(f"Exception: {e}")
                            continue
                        if fetchFlag and not EmptyFlag:
                            pageIndex += 1
                            futureToPageIndex[executor.submit(fetchPage, pageIndex)] = pageIndex
        logger.info(f"Finished getting {num} products from EventID{data_id}")

    def extractEventID(self, URL):
        # extract eventID from URL
        m = re.search(r'event=(\d+)', URL)
        return m.group(1) if m else None
    
