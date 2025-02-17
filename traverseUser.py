from cppDataHandler import cppDataHandler
from cppUserCrawer import cppUserCrawer
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
def isValidUID(uid):
    try:
        test = cppUserCrawer(UID=uid)
        return test
    except Exception as e:
        logger.error(e)
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--number", type=int, 
                        default=100, 
                        help="number of new users to find")
    parser.add_argument("--output", type=str, 
                        default="newUsers.csv", 
                        help="output file")
    parser.add_argument("--userInfo", type=str, 
                        default="users.csv", 
                        help="user info file")
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

    number = args.number
    output = args.output
    userInfo = args.userInfo

    orig_users = pd.read_csv(userInfo)
    maxUID = orig_users["uid"].max()
    count = 0
    newUIDs = set()
    listLock = Lock()
    stopEvent = Event()
    dataHandler = cppDataHandler(csvPath=output, force=args.force)
    logger.info(f"Searching for {number} new users, from {0} to {maxUID*2}")

    with tqdm(total=number) as pbar:
        def parallelCheck(uid):
            nonlocal newUIDs, listLock, number, count, stopEvent
            if stopEvent.is_set():
                return

            if uid in newUIDs or uid in orig_users["uid"].values:
                return
            logger.info(f"Checking UID: {uid}")
            user = isValidUID(uid)
            if user != None and not user.invalid:
                with listLock:
                    if stopEvent.is_set():
                        return
                    count += 1
                    pbar.update(1)
                    newUIDs.add(user.UID)
                    logger.info(f"Found new user: {user.UID}, {count}/{number}")
                    dataHandler.writeCSV(user.getInfo())
                    if count >= number:
                        stopEvent.set()

        maxWorkers = number if number < 20 else 20
        while count < number:
            with concurrent.futures.ThreadPoolExecutor(max_workers=maxWorkers) as executor:
                futures = {executor.submit(parallelCheck, np.random.randint(1, maxUID * 2)): i for i in range(number*2)}
                for _ in concurrent.futures.as_completed(futures):
                    if count >= number:
                        stopEvent.set()
                        executor.shutdown(wait=False)
                        break
    
    logger.info(f"Found {len(newUIDs)} new users")
    return

if __name__ == "__main__":
    main()
