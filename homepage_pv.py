#!/usr/bin/python
import csv
import requests
import xml.etree.ElementTree as ET
import pymysql
from datetime import datetime, timedelta


def main():
    yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    conn = pymysql.connect(host='localhost', user='scraper', password='tiger', db='scraping', charset='utf8')
    try:
        with conn.cursor() as curs:
            sql = """insert into homepage_pv(dt, total, pc, mo, app) values(%s, %s, %s, %s, %s)"""
            loadXML()
            item = parseXML('homepage_pv.xml')
            curs.execute(sql, (yesterday_date,int(item[0]),int(item[1]),int(item[2]),int(item[3])))
        conn.commit()
        with conn.cursor() as curs:
            sql = "select * from homepage_pv"
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
    url = 'https://s3.ap-northeast-2.amazonaws.com/kbs-news-report/pageview_fix/Pageview_' + yesterday_date + '.xml'
    response = requests.get(url)
    with open('homepage_pv.xml', 'wb') as f:
        f.write(response.content)

def parseXML(xmlfile):
    tree = ET.parse(xmlfile)
    root = tree.getroot()
    items = []

    for item in root:
        items.append(item[0].text) #TotalViews
        items.append(item[1].text) #PcViews
        items.append(item[2].text) #MobileView
        items.append(item[3].text) #AppViews

    return items

if __name__ == "__main__":
        main()
