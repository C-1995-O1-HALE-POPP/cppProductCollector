
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest
from util.TimeService import TimeService
from util.CookieManager import CookieManager

from cppEventCrawer import cppEventCrawer
from cppDataHandler import cppDataHandler
from cppCircleCrawer import cppCircleCrawer
from cppUserCrawer import cppUserCrawer

from loguru import logger
import pandas as pd
import os
import sys
import argparse
import json


def productIDtoInfo(main_request: CppRequest, productID=2231):
    # https://www.allcpp.cn/d/952820.do#tabType=0
    #使用soup遍历页面
    return


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

    # eventCrawer = cppEventCrawer(URL=args.page)
    # eventProductDataHandler = cppDataHandler(csvPath="eventProduct.csv", force=args.force)
    # eventProductDataHandler.writeCSV(eventCrawer.getInfos())

    # circleCrawer = cppCircleCrawer(URL=args.page)
    # print(circleCrawer.getInfo())
    # for product in circleCrawer.getSchedule():
    #     print(product)

    userCrawer = cppUserCrawer(URL=args.page)
    for i in userCrawer.getSchedule():
        print(i)

if __name__ == "__main__":
    main()


    