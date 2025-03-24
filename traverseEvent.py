from cppDataHandler import cppDataHandler
from cppEventCrawer import cppEventCrawer
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest

import os
import sys 
import concurrent.futures
import pandas as pd
import argparse
import numpy as np
from threading import Lock, Event   
from tqdm import tqdm 
from loguru import logger
from datetime import datetime
import copy
def isValidEventID(eventId):
    try:
        test = cppEventCrawer(eventID=int(eventId))
        return test
    except Exception as e:
        logger.error(e)
        return None

def main():
    logger.remove()
    logger.add(sys.stderr, level="WARNING")
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"traverse_event_{timestamp}.log")
    logger.add(log_file, rotation="50 MB", retention="10 days", compression="zip")
    logger.info(f"log file: {log_file}")
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=str, 
                        default="eventInfo.csv", 
                        help="output file",
                        required=True)
    parser.add_argument("--userSchedule", type=str, 
                        default="", 
                        help="user schedule file")
    parser.add_argument("--productSchedule", type=str, 
                        default="", 
                        help="product schedule file")
    parser.add_argument("--circleSchedule", type=str, 
                        default="", 
                        help="circle schedule file")
    parser.add_argument("--force", type=bool, 
                        default=False, 
                        help="force to replace the output file")
    parser.add_argument("--refresh-cookie", type=bool, 
                        default=False, 
                        help="refresh cookie")
    parser.add_argument("--relogin", type=bool,
                        default=False,
                        help="relogin")
    parser.add_argument("--maxRetry", type=int,
                        default=10,
                        help="max retry attempts for a connection failure. <= 0 for no retry")
    parser.add_argument("--maxRatePerMinute", type=int,
                        default=10000,
                        help="max request attempts within 60s. <= 0 for infinity attempts")
    parser.add_argument("--retryInterval", type=int,
                        default=10,
                        help="max retry interval. <= 0 for no waiting")
    parser.add_argument("--maxWaitTime", type=int,
                        default=5,
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

    output = args.output
    userInfo = args.userSchedule
    productInfo = args.productSchedule
    circleInfo = args.circleSchedule
    eventIds = set()
    try:
        users = pd.read_csv(userInfo)
        for eventId in users["eventMainId"].values:
            eventIds.add(eventId)
    except Exception as e:
        logger.error(e)
        logger.error("No user schedule file found")
    try:
        products = pd.read_csv(productInfo)
        for eventId in products["eventMainId"].values:
            eventIds.add(eventId)
    except Exception as e:
        logger.error(e)
        logger.error("No product schedule file found")
    try:
        circles = pd.read_csv(circleInfo)
        for eventId in circles["eventId"].values:
            eventIds.add(eventId)
    except Exception as e:
        logger.error(e)
        logger.error("No circle schedule file found")
    if len(eventIds) == 0:
        logger.error("No eventId found")
        exit(1)
    
    logger.warning(f"Searching for {len(eventIds)} events")

    listLock, writerLock = Lock(), Lock()
    stopEvent = Event()
    dataHandler = cppDataHandler(path=output, force=args.force, db_id='eventInfo')
    length = len(eventIds)
    with tqdm(total=length) as pbar:
        def parallelCheck():
            nonlocal eventIds, listLock
            with listLock:
                if stopEvent.is_set() or len(eventIds) == 0:
                    return
                eventId = eventIds.pop()
                if len(eventIds) == 0:
                    stopEvent.set()
                pbar.update(1)
            logger.debug(f"Checking eventId: {eventId}")
            event = isValidEventID(eventId)

            if event is not None:
                with writerLock:
                    logger.debug(f"Found new eventId: {event.eventID}, has session: {event.data_ids}")
                    dataHandler.writeAll(event.getInfos())

        maxWorkers = min(20, length)
        while len(eventIds) != 0:
            current_length = len(eventIds)
            with concurrent.futures.ThreadPoolExecutor(max_workers=maxWorkers) as executor:
                futures = {executor.submit(parallelCheck): i for i in range(current_length)}
                for _ in concurrent.futures.as_completed(futures):
                    if len(eventIds) == 0:
                        stopEvent.set()
                        executor.shutdown(wait=False)
                        break
    
    logger.warning(f"Found {length} new users")
    return

if __name__ == "__main__":
    main()
