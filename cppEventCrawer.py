import gradio as gr
from loguru import logger
from util.KVDatabase import KVDatabase
from util.CppRequest import CppRequest
from util.TimeService import TimeService
from util.CookieManager import CookieManager
import os
import sys
import argparse
import re
import json
import pandas as pd
from bs4 import BeautifulSoup

class cppEventCrawer:
    def __init__(self):
        # 读取 cookie 配置
        config_path = os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "config.json")
        configDB = KVDatabase(config_path)

        if not configDB.contains("cookie_path"):
            configDB.insert("cookie_path", os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "cookies.json"))
        
        cookie_path = configDB.get("cookie_path")

        # 创建 CppRequest 并加载 Cookie
        self.main_request = CppRequest(cookies_config_path=cookie_path)
        self.global_cookieManager = self.main_request.cookieManager

        self.event_url = "https://www.allcpp.cn/allcpp/event/event.do?event="
        self.eventID = 1074
        self.dataIDs = []
        self.soup = None
        return

    def _setEventID(self, eventID):
        self.eventID = eventID
        self.event_url = "https://www.allcpp.cn/allcpp/event/event.do?event=" + str(eventID)
        response = self.main_request.get(self.event_url)
        self.soup = BeautifulSoup(response.text, 'html.parser')
        logger.info("eventID: " + str(eventID))
        return
               
    def _getDataIDs(self):
        if self.soup == None:
            logger.error(__name__, "page is not loaded")
            return []
        logger.info("正在获取 DataIDs, from: " + self.event_url)
        spans = self.soup.select("div.event-days span")
        self.data_ids = [span.get("data-id") for span in spans if span.get("data-id")]
        logger.info("DataIDs: " + str(self.data_ids))
        return self.data_ids

    def getEventID(self):
        return self.eventID
    
    def getEventInfo(self):
        if self.soup == None:
            logger.error(__name__, "page is not loaded")
            return
        logger.info("正在获取活动信息, from: " + self.event_url)
        # 1. 活动名称
        event_name_tag = self.soup.find('h1', class_='info-show-title')
        if event_name_tag:
            event_name = event_name_tag.get_text(strip=True).replace("ONLY ·", "").strip()
        else:
            event_name = None

        # 2. 活动时间（取“时间：”后面的部分）
        time_label = self.soup.find('label', class_='info-show-con')
        event_time = None
        if time_label and "时间：" in time_label.get_text():
            event_time = time_label.get_text(strip=True).split("时间：")[-1].strip()

        # 3. 活动地点及活动城市（取“地点：”后面的部分）
        location_label = self.soup.find('label', class_='info-show-con textDian')
        if location_label and "地点：" in location_label.get_text():
            location_text = location_label.get_text(strip=True).split("地点：")[-1].strip()
            # 如果格式为 "上海 | 徐汇区漕宝路36号E座36space"
            if "|" in location_text:
                event_city, event_location = [s.strip() for s in location_text.split("|", 1)]
            else:
                event_city = None
                event_location = location_text
        else:
            event_city = None
            event_location = None

        # 4. 活动简介（取 id="des" 下 class="text-small" 的内容）
        desc_div = self.soup.find('div', class_='text-small')
        if desc_div:
            event_desc = desc_div.get_text(separator="\n", strip=True)
        else:
            event_desc = None

        # 5. 活动封面（从 id="event-cover" 的 style 属性中提取 URL）
        cover_div = self.soup.find('div', id='event-cover')
        cover_url = None
        if cover_div:
            style_attr = cover_div.get('style', '')
            m = re.search(r'url\((.*?)\)', style_attr)
            if m:
                cover_url = m.group(1).strip(' "\'')

        # 6. 主办方和主办方ID（在 id="event-user-con" 内查找 <a> 标签）
        event_user_div = self.soup.find('div', id='event-user-con')
        event_host = None
        host_id = None
        if event_user_div:
            a_tag = event_user_div.find('a', href=re.compile(r'/u/'))
            if a_tag:
                event_host = a_tag.get_text(strip=True)
                href = a_tag.get('href', '')
                host_match = re.search(r'/u/(\d+)\.do', href)
                if host_match:
                    host_id = host_match.group(1)

        # 7. 相关标签（在 id="r-tag-list" 内提取所有 <a> 的文本）
        tags = []
        tag_ul = self.soup.find('ul', id='r-tag-list')
        if tag_ul:
            for a in tag_ul.find_all('a'):
                tag_text = a.get_text(strip=True)
                tags.append(tag_text)

        # 8. 从脚本中提取部分变量，如结束时间、是否独家、活动类型等
        event_param = {}
        for script in self.soup.find_all('script'):
            if script.string and "var eventParam=" in script.string:
                script_text = script.string
                # 提取结束时间（eDate）
                m = re.search(r'eventParam\.eDate\s*=\s*"([^"]+)"', script_text)
                if m:
                    event_param["eDate"] = m.group(1)
                # 提取是否独家 isOnly
                m = re.search(r'eventParam\.isOnly\s*=\s*(\d+)', script_text)
                if m:
                    event_param["isOnly"] = m.group(1)
                # 提取活动类型 eventType
                m = re.search(r'eventParam\.eventType\s*=\s*(\d+)', script_text)
                if m:
                    event_param["eventType"] = m.group(1)
                # 提取活动城市（备用）
                m = re.search(r'eventParam\.eventCity\s*=\s*"([^"]+)"', script_text)
                if m:
                    event_param["eventCity"] = m.group(1)
                # 提取开始时间（sDate）
                m = re.search(r'eventParam\.sDate\s*=\s*"([^"]+)"', script_text)
                if m:
                    event_param["sDate"] = m.group(1)
                break  # 假设只需处理第一个包含 eventParam 的脚本

        return {"event_name": event_name,
                "event_time": event_time,
                "event_city": event_param.get('eventCity', event_city), 
                "event_location": event_location, 
                "event_desc": event_desc, 
                "cover_url": cover_url, 
                "event_host": event_host, 
                "host_id": host_id, 
                "tags": tags, 
                "isOnly": event_param.get('isOnly', '未知'), 
                "eventType": event_param.get('eventType', '未知')}
    
    def downloadProducts(self, csvFilename = ""):
        if self.data_ids == []:
            logger.warning("没有获取到 DataIDs，无法下载产品信息")
            return
        if csvFilename == "":
            csvFilename = f"eventID{self.eventID}_products.csv"
        firstWrite = True
        num = 0
        for data_id in self.data_ids:
            isEmptyPage = False
            pageIndex = 1
            while not isEmptyPage:
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
                    logger.error("eventToProduct: api请求失败")
                    sys.exit(1)

                data = json.loads(response.text)
                if not data["isSuccess"]:
                    logger.error("eventToProduct: 服务器请求失败")
                    sys.exit(1)

                # 获取当前页数据列表
                pageList = data["result"]["list"]
                isEmptyPage = len(pageList) == 0

                # 如果当前页有数据，直接追加写入 CSV 文件
                if not isEmptyPage:
                    df = pd.DataFrame(pageList)
                    if firstWrite:
                        # 第一次写入 CSV，同时写入标题行
                        df.to_csv(csvFilename, index=False, mode='w', encoding='utf-8')
                        firstWrite = False
                    else:
                        # 追加写入，不写入标题行
                        df.to_csv(csvFilename, index=False, mode='a', header=False, encoding='utf-8')
                    num += len(pageList)
                    logger.info(f"from {data_id}, 第 {pageIndex} 页写入 {len(pageList)} 条数据")
                pageIndex += 1

        logger.info(f"数据已写入 {csvFilename}, 共 {num} 条数据")

        # 如果文件中有数据，读取并打印第一行（不含 header）
        if not firstWrite:
            df = pd.read_csv(csvFilename, encoding='utf-8')
            if not df.empty:
                first_row = df.iloc[0]
                logger.info("CSV第一行数据:")
                logger.info(first_row)
        else:
            logger.warning("没有获取到数据，未生成 CSV 文件")
        return
    
    def loadEventFromURL(self, URL = "https://www.allcpp.cn/allcpp/event/event.do?event=2231"):
        eventID = int(URL.split("event=")[-1])
        self._setEventID(eventID)
        self._getDataIDs()
        return self.getEventInfo()
    