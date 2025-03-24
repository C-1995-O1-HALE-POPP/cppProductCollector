
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
from threading import Lock
from datetime import datetime
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
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    # 日志文件目录
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 日志文件名（带时间戳 + eventID 或自定义）
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"main_event_{timestamp}.log")

    # 日志配置：输出到文件
    logger.add(log_file, rotation="50 MB", retention="10 days", compression="zip")
    logger.info(f"日志文件已启用: {log_file}")
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
    
    if args.maxRetry <= 0:
        logger.warning("maxRetry <= 0, no retry attempts. Good luck!")
    if args.maxRatePerMinute <= 0:
        logger.warning("maxRatePerMinute <= 0, infinity attempts. Good luck!")
    if args.retryInterval <= 0:
        logger.warning("retryInterval <= 0, no waiting. Good luck!")
    if args.maxWaitTime <= 0:
        logger.warning("maxWaitTime <= 0, no timeout. Good luck!")

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
    print(eventCrawer.data_ids)

    eventId = eventCrawer.getEventID()
    productEventDataHandler = cppDataHandler(path=f"{eventId}_Event_products", db_id=f'{eventId}',force=args.force)
    #circleEventDataHandler = cppDataHandler(path=f"{eventId}_Event_circles", db_id=f'{eventId}',force=args.force)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        #future1 = executor.submit(circleEventDataHandler.writeAll, eventCrawer.getCircles())
        future2 = executor.submit(productEventDataHandler.writeAll, eventCrawer.getProducts())
        concurrent.futures.wait([future2])
    logger.warning(f"Event {eventId} has been loaded")

    dataProducts = pd.read_csv(f"{eventId}_Event_products.csv")
    dataCircle = pd.read_csv(f"{eventId}_Event_circles.csv")

    # get circles and products entries from the Event
    allcircles = dataCircle["id"].unique()
    allproducts = dataProducts["doujinshiId"].unique()
    logger.info(f"Total {len(allcircles)} circles and {len(allproducts)} products in the event")

    # dataHandlers
    circleDataHandler = cppDataHandler(path=f"{eventId}_Circles_Info",db_id=f'{eventId}', force=args.force)
    circleProductsDataHandler = cppDataHandler(path=f"{eventId}_Circle_ALL_Products",db_id=f'{eventId}', force=args.force)
    circleScheduleDataHandler = cppDataHandler(path=f"{eventId}_Circle_Schedule",db_id=f'{eventId}', force=args.force)

    productDataHandler = cppDataHandler(path=f"{eventId}_Products_Info1",db_id=f'{eventId}', force=args.force)
    productScheduleDataHandler = cppDataHandler(path=f"{eventId}_Product_Schedule1",db_id=f'{eventId}', force=args.force)

    userDataHandler = cppDataHandler(path=f"{eventId}_User_Info",db_id=f'{eventId}', force=args.force)
    userScheduleDataHandler = cppDataHandler(path=f"{eventId}_user_Schedule",db_id=f'{eventId}', force=args.force)
    userProduceDataHandler = cppDataHandler(path=f"{eventId}_user_ALL_Products",db_id=f'{eventId}', force=args.force)

    # get all UID from circles
    user_ids = []
    for entry in tqdm(dataCircle["circleMemberList"]):
        user_ids.extend(re.findall(r"'userId': (\d+)", entry))
    user_ids = list(set(map(int, user_ids)))  # unique
    logger.info(f"Total {len(user_ids)} users in the event")

    lock1, lock2, lock3 = Lock(), Lock(), Lock()
    def process_circle(circle):
        circleCrawer = cppCircleCrawer(circle)
        with lock1:
            circleDataHandler.writeAll(circleCrawer.getInfo())
            circleProductsDataHandler.writeAll(circleCrawer.getProducts())
            circleScheduleDataHandler.writeAll(circleCrawer.getSchedule())
    num = 0
    length = len(allproducts)

    def process_product(product):
        productCrawer = cppProductCrawer(product)
        with lock2:
            productDataHandler.writeAll(productCrawer.getInfo())
            productScheduleDataHandler.writeAll(productCrawer.getSchedule())


    def process_user(uid):
        userCrawer = cppUserCrawer(UID=uid)
        with lock3:
            userDataHandler.writeAll(userCrawer.getInfo())
            userScheduleDataHandler.writeAll(userCrawer.getSchedule())
            userProduceDataHandler.writeAll(userCrawer.getProducts())


    # thread pool for execution
    def execute_parallel_tasks(task_func, task_list, max_workers=100):
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(task_func, task): task for task in task_list}
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                # try:
                    future.result()
                # except Exception as e:
                #     logger.error(f"Error: {e}")
                #     logger.error(f"Task {futures[future]} failed")
                #     continue

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future1 = executor.submit(execute_parallel_tasks, process_user, user_ids, max_workers=20)
        future2 = executor.submit(execute_parallel_tasks, process_product, allproducts, max_workers=10)
        future3 = executor.submit(execute_parallel_tasks, process_circle, allcircles, max_workers=10)
        concurrent.futures.wait([future1, future2, future3])    


if __name__ == "__main__":
    main()


    