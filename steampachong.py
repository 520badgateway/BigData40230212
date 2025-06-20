# steampachong.py
# -*- coding: utf-8 -*-
"""
用途：
从 Steam 商店爬取指定游戏的评论（简体中文），并保存为 csv 文件。

作者：40230212小组
日期：2025-06-19
"""

import requests
import time
import re
import pandas as pd
from bs4 import BeautifulSoup

# 全局配置
appid = input("请输入 appID：").strip()
maxDataSize = int(input("请输入要请求数据的条数：").strip())  # 10的倍数
appName = "Unknown"
listAllContent = []       # 所有评论
nextCursor = "*"          # Steam 翻页 cursor
reloadDataNum = 0         # 请求失败重试计数
reloadDataNumMax = 5      # 最大重试次数
lastedNum = 0             # 上次数据量
totalEndNum = 0           # 连续无新数据计数

# 去掉多余空白
def clean_text(text: str) -> str:
    return re.sub(r'\s+', ' ', text).strip()

# 获取并清洗游戏名称
def getGameName():
    global appName
    try:
        resp = requests.get(f"https://store.steampowered.com/app/{appid}", timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print("获取游戏名字失败：", e)
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    span = soup.find("span", itemprop="name")
    if span and span.string:
        name = span.string
        # 把文件名非法字符替换为下划线
        appName = re.sub(r'[:\\/*?"<>|]', '_', name)
        print("游戏名字:", appName)
    else:
        print("未找到游戏名字:", appName)
    time.sleep(1)

# 拉取一页评论
def getInitCursorValue() -> bool:
    global reloadDataNum, nextCursor, lastedNum, totalEndNum

    url = (
        f"https://store.steampowered.com/appreviews/{appid}"
        f"?cursor={nextCursor}&language=schinese&day_range=365"
        "&review_type=all&purchase_type=all&filter=recent"
    )

    try:
        js = requests.get(url, timeout=10).json()
    except Exception:
        reloadDataNum += 1
        print(f"请求评论失败，尝试重新拉取...{reloadDataNum}")
        time.sleep(1)
        return reloadDataNum >= reloadDataNumMax

    reloadDataNum = 0

    # 解析 HTML
    soup = BeautifulSoup(js.get("html", ""), "html.parser")
    names = [a.string for d in soup.select("div.persona_name") for a in d.select("a")]
    recs = [clean_text(d.text) for d in soup.select("div.title.ellipsis")]
    times = [clean_text(d.text) for d in soup.select("div.hours.ellipsis")]
    comments = [clean_text(d.text) for d in soup.select("div.content")]

    # 合并
    for name, rec, tm, com in zip(names, recs, times, comments):
        if len(listAllContent) >= maxDataSize:
            break
        listAllContent.append([name, rec, tm, com])

    print(f"请求完成, 当前一共{len(listAllContent)}条")

    # 检查是否无新数据
    if len(listAllContent) == lastedNum:
        totalEndNum += 1
        print(f"请求不到更多评论...{totalEndNum}")
        if totalEndNum >= reloadDataNumMax:
            print("结束请求...")
            return True
    else:
        lastedNum = len(listAllContent)
        totalEndNum = 0

    # 更新 cursor
    cursor = js.get("cursor", "").replace("+", "%2B")
    nextCursor = cursor

    return len(listAllContent) >= maxDataSize

def save_as_csv():
    filename = f"{appid}_{maxDataSize}.csv"
    df = pd.DataFrame(listAllContent, columns=["用户名", "推荐", "游戏时长", "评论"])
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print("存储到", filename)

if __name__ == "__main__":
    getGameName()
    while len(listAllContent) < maxDataSize:
        if getInitCursorValue():
            print("爬取完成")
            break
        time.sleep(0.5)
    save_as_csv()

 
