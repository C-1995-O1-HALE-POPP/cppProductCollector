from loguru import logger
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest
import os
import sys
import re
import json
import concurrent.futures
import threading
class cppUserCrawer:
    def __init__(self, UID = -1, URL = "", maxWorker = 10):
        # read cookie config
        config_path = os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "config.json")
        configDB = KVDatabase(config_path)

        if not configDB.contains("cookie_path"):
            configDB.insert("cookie_path", os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "cookies.json"))
        
        cookie_path = configDB.get("cookie_path")

        # load cookie with CppRequest
        self.main_request = CppRequest(cookies_config_path=cookie_path)
        self.global_cookieManager = self.main_request.cookieManager

        if UID == -1 and URL == "":
            logger.error("UID and URL cannot be empty at the same time")
            exit(1)
        elif UID == -1:
            UID = self.extractUID(URL)
            if UID == None:
                logger.error("Bad URL")
                exit(1)
        # get APIs, fortunately, we can get all information with them
        self.UID = UID
        self.infoApi = f"https://www.allcpp.cn/allcpp/loginregister/getUser/{self.UID}.do?"
        response = self.main_request.get(self.infoApi)
        if response.status_code != 200:
            logger.error("infoApi request failed" + str(self.UID))
            return
        data = json.loads(response.text)
        if not data["isSuccess"]:
            logger.error("bad response" + str(self.UID))
            return
        self.data = data["result"]

        logger.info("Successfully load UID" + str(self.UID))
        
        self.maxWorker = maxWorker
        self.lock = threading.Lock()
        return

    def extractUID(self, url):
        # extract UID from url
        match = re.search(r"/u/(\d+)\.do", url)
        return match.group(1) if match else None
    
    def getInfo(self):
        # get user's information, compress paticipated circleIds to circleList
        info = self.data["userMain"].copy()
        info["circleList"] = [circle["circleId"] for circle in self.data["circleList"]]
        return info
    
    def getProducts(self, limitation = -1):
        # get user's products
        num = 0
        pageIndex = 1
        fetchFlag = True
        isEmptyPage = False

        def fetchPage(pageIndex):
            nonlocal fetchFlag
            if not fetchFlag:
                return []
            worker_name = threading.current_thread().name
            productsApi = "".join(["https://www.allcpp.cn/allcpp/doujinshi/getAuthorDoujinshiList.do",
                            "?pageindex=",
                            str(pageIndex),
                            "&pagesize=2000&userid=",
                            str(self.UID),
                            "&searchstring=&canupdate=-1&havecreater=1"])
            response = self.main_request.get(productsApi)
            if response.status_code != 200:
                logger.error(f"[{worker_name}] productsApi request failed" + str(self.UID))
                return[]
            data = json.loads(response.text)
            if not data["isSuccess"]:
                logger.error(f"[{worker_name}] bad response" + str(self.UID))
                return[]
            pageList = data["result"]["list"]
            if pageList == [] or pageList == None:
                return []
            logger.info(f"[{worker_name}] Getting Page {pageIndex}, {len(pageList)} products from UID{self.UID}")
            for product in pageList:
                product["userId"] = self.UID
            return pageList
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.maxWorker) as executor:
            futureToPageIndex = {executor.submit(fetchPage, pageIndex): pageIndex}
            while fetchFlag and not isEmptyPage and futureToPageIndex:
                for future in concurrent.futures.as_completed(futureToPageIndex):
                    pageIndex = futureToPageIndex.pop(future)
                    try:
                        product = future.result()
                        if product == [] or product == None:
                            isEmptyPage = True
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
                    if fetchFlag and not isEmptyPage:
                        pageIndex += 1
                        futureToPageIndex[executor.submit(fetchPage, pageIndex)] = pageIndex

            
    
    def getSchedule(self):
        # get user's schedule
        num = 0
        fetchFlag = True
        for isnew, iswannago in [(1, 1), (0, 1), (1, 0), (0, 0)]:
            isEmptyPage = False
            pageIndex = 1
            while fetchFlag and not isEmptyPage:
                scheduleApi = "".join(["https://www.allcpp.cn/allcpp/user/getUserEventList.do?userid=",
                                    str(self.UID),
                                    "&pageindex=",
                                    str(pageIndex),
                                    "&pagesize=20&isnew=",
                                    str(isnew),
                                    "&iswannago=",
                                    str(iswannago)])
                response = self.main_request.get(scheduleApi)
                if response.status_code != 200:
                    logger.error("scheduleApi request failed" + str(self.UID))
                    return
                data = json.loads(response.text)
                if not data["isSuccess"]:
                    logger.error("bad response" + str(self.UID))
                    return
                pageList = data["result"]["list"]
                isEmptyPage = len(pageList) == 0
                logger.info(f"Getting Page {pageIndex}, {len(pageList)} schedule [isnew, iswannago] = {[isnew, iswannago]} from UID{self.UID}")
                for event in pageList:
                    yield {"uid":self.UID, 
                           "isNew":isnew, 
                           "isWannaGo":iswannago, 
                           "eventId":event["id"], 
                           "eventMainId":event["eventMainId"]}
                    num += 1
                pageIndex += 1