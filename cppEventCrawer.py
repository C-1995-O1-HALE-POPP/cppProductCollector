from loguru import logger
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest
import os
import sys
import re
import json


class cppEventCrawer:
    def __init__(self, eventID = -1, URL = ""):
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
        self.event_url = "https://www.allcpp.cn/allcpp/event/event.do?event=" + str(eventID)
        self.eventApi = "https://www.allcpp.cn/allcpp/event/getevents.do"

        # get event status from api
        response = self.main_request.post(self.eventApi, data=json.dumps({"id": eventID}))
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
    
    def getProducts(self, limitation=-1):
        # get products with api, limitation is the maximum number of products to return
        if self.data_ids == []:
            logger.warning("No available DataIDs, getProducts failed")
            return
        num = 0
        fetchFlag = True
        for data_id in self.data_ids:
            isEmptyPage = False
            pageIndex = 1
            while not isEmptyPage and fetchFlag:
                dojin_api = ''.join([
                    "https://www.allcpp.cn/api/doujinshi/search.do?eventId=",
                    str(data_id),
                    "&keyword=&orderBy=0&typeIds=36%2C37%2C38%2C39%2C40%2C50%2C51%2C52",
                    "&pageIndex=",
                    str(pageIndex),
                    "&pageSize=600&sellStatus=&ideaStatus=&tag="
                ])
                response = self.main_request.get(dojin_api)

                if response.status_code != 200:
                    logger.error("api request failed")
                    return

                data = json.loads(response.text)
                if not data["isSuccess"]:
                    logger.error("bad response")
                    return

                # get list on current page
                pageList = data["result"]["list"]
                isEmptyPage = len(pageList) == 0
                logger.info(f"Getting Page {pageIndex}, {len(pageList)} products from EventID{data_id}")
                for product in pageList:
                    product["eventId"] = self.eventID
                    product["dataId"] = data_id
                    if limitation != -1 and num >= limitation:
                        fetchFlag = False
                        break
                    yield product
                    num += 1
                pageIndex += 1

    def extractEventID(self, URL):
        # extract eventID from URL
        m = re.search(r'event=(\d+)', URL)
        return m.group(1) if m else None
    
