from loguru import logger
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest
import os
import sys
import re
import json

class cppProductCrawer:
    def __init__(self, PID = -1, URL = ""):
        # read cookie config
        config_path = os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "config.json")
        configDB = KVDatabase(config_path)

        if not configDB.contains("cookie_path"):
            configDB.insert("cookie_path", os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "cookies.json"))
        
        cookie_path = configDB.get("cookie_path")

        # load cookie with CppRequest
        self.main_request = CppRequest(cookies_config_path=cookie_path)
        self.global_cookieManager = self.main_request.cookieManager

        if PID == -1 and URL == "":
            logger.error("UID and URL cannot be empty at the same time")
            exit(1)
        elif PID == -1:
            PID = self.extractPID(URL)
            if PID == None:
                logger.error("Bad URL")
                exit(1)
        # get APIs, fortunately, we can get all information with them
        self.PID = PID
        self.infoApi = f"https://www.allcpp.cn/allcpp/djs/detail.do?doujinshiID={self.PID}"
        request = self.main_request.get(self.infoApi)
        if request.status_code != 200:
            logger.error("infoApi request failed" + str(self.PID))
            return
        data = json.loads(request.text)
        if not data["isSuccess"]:
            logger.error("bad response" + str(self.PID))
            return
        self.info = data["result"]

        logger.info("Successfully load PID" + str(PID))
        return

    def extractPID(self, url):
        # extract UID from url
        match = re.search(r"/d/(\d+)\.do", url)
        return match.group(1) if match else None
    
    def getInfo(self):
        return self.info
    
    def getSchedule(self):
        # get user's schedule
        num = 0
        for isnew in [0, 1]:
            scheduleApi = "".join(["https://www.allcpp.cn/allcpp/djs/joinedEvent.do?doujinshiid=",
                                    str(self.PID),
                                    "&isnew=",
                                    str(isnew)])
            response = self.main_request.get(scheduleApi)
            if response.status_code != 200:
                logger.error("scheduleApi request failed" + str(self.PID))
                return
            data = json.loads(response.text)
            if not data["isSuccess"]:
                logger.error("bad response" + str(self.PID))
                return
            pageList = data["result"]
            logger.info(f"Getting Page {len(pageList)} schedule, [isnew] = {isnew} from PID{self.PID}")
            for event in pageList:
                event["isnew"] = isnew
                num += 1
                yield event
        logger.info(f"Successfully get {num} schedule(s) from PID{self.PID}")