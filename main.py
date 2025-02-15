
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest
from util.TimeService import TimeService
from util.CookieManager import CookieManager

from cppEventCrawer import cppEventCrawer
from cppDataHandler import cppDataHandler
from cppCircleCrawer import cppCircleCrawer
from cppUserCrawer import cppUserCrawer
from cppProductCrawer import cppProductCrawer

from loguru import logger
import pandas as pd
import os
import sys
import argparse
import json

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser()
    parser.add_argument("--page", type=str, 
                        default="https://www.allcpp.cn/allcpp/event/event.do?event=2231", 
                        help="page url")
    parser.add_argument("--output", type=str, 
                        default="", 
                        help="output file path")
    parser.add_argument("--refresh-cookie", type=bool, 
                        default=False, 
                        help="refresh cookie")
    parser.add_argument("--relogin", type=bool,
                        default=False,
                        help="relogin")
    parser.add_argument("--force", type=bool,
                        default=False,
                        help="force to replace the output file")
                        

    args = parser.parse_args()

    # 读取 cookie 配置
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

    # eventCrawer = cppEventCrawer(URL=args.page)
    # eventProductDataHandler = cppDataHandler(csvPath="eventProduct.csv", force=args.force)
    # eventProductDataHandler.writeCSV(eventCrawer.getInfos())

    # circleCrawer = cppCircleCrawer(URL=args.page)
    # print(circleCrawer.getInfo())
    # for product in circleCrawer.getSchedule():
    #     print(product)

    # userCrawer = cppUserCrawer(URL=args.page)
    # for i in userCrawer.getProducts(3):
    #     print(i)

    productCrawer = cppProductCrawer(URL=args.page)
    print(productCrawer.getInfo())
    for i in productCrawer.getSchedule():
        print(i)

        # TODO: 明确任务

if __name__ == "__main__":
    main()


    