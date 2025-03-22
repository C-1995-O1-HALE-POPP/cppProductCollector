from loguru import logger
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest
import os
import sys
import re
import json
from datetime import datetime

class cppCircleCrawer:
    def __init__(self, circleID = -1, URL = ""):
        # read cookie config
        config_path = os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "config.json")
        configDB = KVDatabase(config_path)

        if not configDB.contains("cookie_path"):
            configDB.insert("cookie_path", os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "cookies.json"))
        
        cookie_path = configDB.get("cookie_path")

        # load cookie with CppRequest
        self.main_request = CppRequest(cookies_config_path=cookie_path)
        self.global_cookieManager = self.main_request.cookieManager

        if circleID == -1 and URL == "":
            logger.error("circleID and URL cannot be empty at the same time")
            exit(1)
        elif circleID == -1:
            circleID = self.extractCircleId(URL)
            if circleID == None:
                logger.error("Bad URL")
                exit(1)
        # get circle page
        myUID = KVDatabase(config_path).get("UID")
        self.circleID = circleID
        self.scheduledEventsApi = f"https://www.allcpp.cn/allcpp/circle/mainEvent.do?circle_id={self.circleID}&startDate=2010-02-01"
        self.circleInfoApi = f"https://www.allcpp.cn/api/circle/getcircledetail.do?circleid={self.circleID}&userid={myUID}"
        response = self.main_request.get(self.circleInfoApi)
        if response.status_code != 200:
            logger.error("circlePage request failed" + str(circleID))
            return
        data = json.loads(response.text)
        if not data["isSuccess"]:
            logger.error("bad response" + str(circleID))
            return
        self.data = data["result"]
        logger.info("Successfully load circleID" + str(circleID))

        return


    def getCircleID(self):
        # return circleID
        return self.circleID
    
    
    def getSchedule(self, limitation = -1):
        # get scheduled events with api, limitation is the maximum number of events to return
        response = self.main_request.get(self.scheduledEventsApi)
        if response.status_code != 200:
            logger.error("api request failed")
            sys.exit(1)
        data = response.json()
        if not data["isSuccess"]:
            logger.error("Bad response")
            sys.exit(1)

        num = 0
        for event in data["result"]:
            if limitation > 0 and num >= limitation:
                break
            event_id = event["id"]
            event_name = event["name"]
            enter_time = datetime.fromtimestamp(event["enterTime"] / 1000).strftime('%Y-%m-%d')
            yield {"circleId": self.circleID, 
                   "eventId": event_id, 
                   "eventName": event_name, 
                   "enterTime": enter_time}
            num += 1
        
    def getProducts(self, limitation = -1):
        # get products with api, limitation is the maximum number of products to return
        num = 0
        fetchFlag = True
        pageIndex = 1
        isEmptyPage = False
        while not isEmptyPage and fetchFlag:
            circleProductsApi = ''.join(["https://www.allcpp.cn/allcpp/circle/allBenZi.do?circle_id=",
                                         str(self.circleID),
                                         "&sub_event_id=0&page=",
                                         str(pageIndex),
                                         "&pageSize=600"])
            response = self.main_request.get(circleProductsApi)
            if response.status_code != 200:
                logger.error("api request failed")
                sys.exit(1)
            try:
                data = json.loads(response.text)
            except:
                logger.error("bad response" + str(self.PID))
                break
            if not data["isSuccess"]:
                logger.error("bad response")
                sys.exit(1)
            pageList = data["result"]["rows"]
            isEmptyPage = (len(pageList) == 0)
            logger.debug(f"Getting Page {pageIndex}, {len(pageList)} products from CircleID{self.circleID}")
            for product in pageList:
                product["circleId"] = self.circleID
                if limitation != -1 and num >= limitation:
                    fetchFlag = False
                    break
                yield product
                num += 1
            pageIndex += 1

    def extractCircleId(self, url):
        # extract circleID from url
        match = re.search(r"/c/(\d+)\.do", url)
        return match.group(1) if match else None
    
    def getInfo(self):
        # return circle information
        return self.data