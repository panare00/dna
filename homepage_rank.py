#!/usr/bin/python
import csv
import requests
import xml.etree.ElementTree as ET
import pymysql
from datetime import datetime, timedelta


def main():
    loadXML()
    tree = ET.parse('homepage_rank.xml')
    root = tree.getroot()

    yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    conn = pymysql.connect(host='localhost', user='scraper', password='tiger', db='scraping', charset='utf8')
    try:
        with conn.cursor() as curs:
            sql = """insert into homepage_rank(dt, rank, newsCode, newsTitle, url, sourceName, contentsName, broadName, positionName, reporterName, totalViews, pcViews, mobileViews, appViews) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            for item in root.findall("Item"):
                curs.execute(sql, (
                    yesterday_date,
                    int(item.find("Rank").text),
                    item.find("NewsCode").text,
                    item.find("NewsTitle").text,
                    item.find("Url").text,
                    item.find("SourceName").text,
                    item.find("ContentsName").text,
                    item.find("BroadName").text,
                    item.find("PositionName").text,
                    item.find("ReporterName").text,
                    int(item.find("TotalViews").text),
                    int(item.find("PcViews").text),
                    int(item.find("MobileViews").text),
                    int(item.find("AppViews").text)
                ))
        conn.commit()
        with conn.cursor() as curs:
            sql = "select * from homepage_rank"
            curs.execute(sql)
            rs = curs.fetchall()
            for row in rs:
                print(row)

    #except Error as error:
    #    print(error)
    finally:
        conn.close()

def loadXML():
    yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
    url = 'http://kbs-news-report.s3-website.ap-northeast-2.amazonaws.com/pageview/TopRankNews_'+yesterday_date+'.xml'
    response = requests.get(url)
    with open('homepage_rank.xml', 'wb') as f:
        f.write(response.content)

if __name__ == "__main__":
        main()
