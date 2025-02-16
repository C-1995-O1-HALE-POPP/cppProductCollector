
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest
from util.TimeService import TimeService
from util.CookieManager import CookieManager

from cppEventCrawer import cppEventCrawer
from cppDataHandler import cppDataHandler
from cppCircleCrawer import cppCircleCrawer
from cppUserCrawer import cppUserCrawer
from cppProductCrawer import cppProductCrawer

import concurrent.futures
from loguru import logger
import pandas as pd
import os
import sys
import argparse
import json
import time
import re
from tqdm import tqdm 

def main():
    # get args
    parser = argparse.ArgumentParser()
    parser.add_argument("--page", type=str, 
                        default="https://www.allcpp.cn/allcpp/event/event.do?event=2231", 
                        help="page url")
    parser.add_argument("--refresh-cookie", type=bool, 
                        default=False, 
                        help="refresh cookie")
    parser.add_argument("--relogin", type=bool,
                        default=False,
                        help="relogin")
    parser.add_argument("--force", type=bool,
                        default=False,
                        help="force to replace the output file")
    parser.add_argument("--maxRetry", type=int,
                        default=10,
                        help="max retry attempts for a connection failure. <= 0 for no retry")
    parser.add_argument("--maxRatePerMinute", type=int,
                        default=100,
                        help="max request attempts within 60s. <= 0 for infinity attempts")
    parser.add_argument("--retryInterval", type=int,
                        default=30,
                        help="max retry interval. <= 0 for no waiting")
    parser.add_argument("--maxWaitTime", type=int,
                        default=10,
                        help="max time to wait for a request. <= 0 for no timeout")

    args = parser.parse_args()

    # get cookie config
    config_path = os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "config.json")
    configDB = KVDatabase(config_path)

    if not configDB.contains("cookie_path"):
        configDB.insert("cookie_path", os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "cookies.json"))
    
    cookie_path = configDB.get("cookie_path")

    main_request = CppRequest(cookies_config_path=cookie_path)

    if args.refresh_cookie:
        global_cookieManager = main_request.cookieManager
        global_cookieManager.refreshToken()

    if args.relogin:
        global_cookieManager = main_request.cookieManager
        global_cookieManager.clear_cookies()

    configDB.insert("maxRetry", args.maxRetry)
    configDB.insert("maxRatePerMinute", args.maxRatePerMinute)
    configDB.insert("retryInterval", args.retryInterval)
    configDB.insert("maxWaitTime", args.maxWaitTime)

    # get selfUID
    request = main_request.get("https://www.allcpp.cn/allcpp/circle/getCircleMannage.do")
    if request.status_code != 200:
        logger.error("UserApi request failed")
        exit(1)
    data = json.loads(request.text)
    if not data["isSuccess"]:
        logger.error("bad response")
        exit(1)
    KVDatabase(config_path).insert("UID", data["result"]["joinCircleList"][0]["userId"])
    logger.info("Successfully login, your UID is " + str(data["result"]["joinCircleList"][0]["userId"]))

    # get event products and event circles
    eventCrawer = cppEventCrawer(URL = args.page)
    eventId = eventCrawer.getEventID()
    productEventDataHandler = cppDataHandler(csvPath=f"{eventId}_Event_products.csv")
    productEventDataHandler.writeCSV(eventCrawer.getProducts())
    circleEventDataHandler = cppDataHandler(csvPath=f"{eventId}_Event_circles.csv")
    circleEventDataHandler.writeCSV(eventCrawer.getCircles())

    dataProducts = pd.read_csv(f"{eventId}_Event_products.csv")
    dataCircle = pd.read_csv(f"{eventId}_Event_circles.csv")

    # get circles and products entries from the Event
    allcircles = dataCircle["id"].unique()
    allproducts = dataProducts["doujinshiId"].unique()

    # dataHandlers
    circleDataHandler = cppDataHandler(csvPath=f"{eventId}_Circles_Info.csv")
    circleProductsDataHandler = cppDataHandler(csvPath=f"{eventId}_Circle_ALL_Products.csv")
    circleScheduleDataHandler = cppDataHandler(csvPath=f"{eventId}_Circle_Schedule.csv")

    productDataHandler = cppDataHandler(csvPath=f"{eventId}_Products_Info.csv")
    productScheduleDataHandler = cppDataHandler(csvPath=f"{eventId}_Product_Schedule.csv")

    userDataHandler = cppDataHandler(csvPath=f"{eventId}_User_Info.csv")
    userScheduleDataHandler = cppDataHandler(csvPath=f"{eventId}_user_Schedule.csv")
    userProduceDataHandler = cppDataHandler(csvPath=f"{eventId}_user_ALL_Products.csv")

    # get all UID from circles
    user_ids = []
    for entry in tqdm(dataCircle["circleMemberList"]):
        user_ids.extend(re.findall(r"'userId': (\d+)", entry))
    user_ids = list(set(map(int, user_ids)))  # unique


    def process_circle(circle):
        circleCrawer = cppCircleCrawer(circle)
        circleDataHandler.writeCSV(circleCrawer.getInfo())
        circleProductsDataHandler.writeCSV(circleCrawer.getProducts())
        circleScheduleDataHandler.writeCSV(circleCrawer.getSchedule())


    def process_product(product):
        productCrawer = cppProductCrawer(product)
        productDataHandler.writeCSV(productCrawer.getInfo())
        productScheduleDataHandler.writeCSV(productCrawer.getSchedule())


    def process_user(uid):
        userCrawer = cppUserCrawer(UID=uid)
        userDataHandler.writeCSV(userCrawer.getInfo())
        userScheduleDataHandler.writeCSV(userCrawer.getSchedule())
        userProduceDataHandler.writeCSV(userCrawer.getProducts())


    # thread pool for execution
    def execute_parallel_tasks(task_func, task_list, max_workers=10):
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(task_func, task): task for task in task_list}
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                future.result() 


    # go multitasking!
    execute_parallel_tasks(process_circle, allcircles, max_workers=10)
    execute_parallel_tasks(process_product, allproducts, max_workers=10)
    execute_parallel_tasks(process_user, user_ids, max_workers=10)


if __name__ == "__main__":
    main()


    