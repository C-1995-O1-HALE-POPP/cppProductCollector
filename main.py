
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest
from util.TimeService import TimeService
from util.CookieManager import CookieManager
from cppEventCrawer import cppEventCrawer

from loguru import logger
import pandas as pd
import re
import os
import sys
import argparse
from bs4 import BeautifulSoup
import json
def circleIDtoInfo(main_request: CppRequest, circleID=2231):
    # https://www.allcpp.cn/c/5096.do
    #使用soup遍历页面
    return

def circleIDtoProduct(main_request: CppRequest, circleID=2231):
    #api: https://www.allcpp.cn/allcpp/circle/allBenZi.do?circle_id=5096&sub_event_id=0&page=1&pageSize=600
    return

def circleIDtoUser(main_request: CppRequest, circleID=2231):
    # https://www.allcpp.cn/c/5096.do
    #使用soup遍历页面
    return

def userIDtoProduct(main_request: CppRequest, userID=2231):
    #api: https://www.allcpp.cn/allcpp/doujinshi/getAuthorDoujinshiList.do?pageindex=1&pagesize=2000&userid=1560556&searchstring=&canupdate=-1&havecreater=1
    return

def userIDtoInfo(main_request: CppRequest, userID=2231):
    # api: https://www.allcpp.cn/allcpp/loginregister/getUser/1560556.do?
    return

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
    parser.add_argument("--reload-cookie", type=bool, 
                        default=False, 
                        help="login again to get new cookie")

    args = parser.parse_args()

    # 读取 cookie 配置
    config_path = os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "config.json")
    configDB = KVDatabase(config_path)

    if not configDB.contains("cookie_path"):
        configDB.insert("cookie_path", os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "cookies.json"))
    
    cookie_path = configDB.get("cookie_path")

    if args.reload_cookie:
        main_request = CppRequest(cookies_config_path=cookie_path)
        global_cookieManager = main_request.cookieManager
        global_cookieManager.refreshToken()

    eventCrawer = cppEventCrawer()
    eventCrawer.loadEventFromURL(args.page)
    eventCrawer.downloadProducts(args.output)




if __name__ == "__main__":
    main()


    