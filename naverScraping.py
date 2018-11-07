#!/usr/bin/python
import requests
from bs4 import BeautifulSoup
import lxml.html
import pymysql
import time

def main():

        conn = pymysql.connect(host='localhost', user='scraper', password='tiger', db='scraping', charset='utf8')

        try:
                with conn.cursor() as curs:
                        sql = """insert into naver_article(title, url, dt, category, rank, pv) values (%s, %s, %s, %s, %s, %s)"""

                        session = requests.session()
                        response = session.get('http://cis.kbs.co.kr/cis/favorite_naver_mobile-3.html')
                        root = lxml.html.fromstring(response.content)
                        root.make_links_absolute(response.url)

                        time.sleep(2)

                        dt = root.cssselect('input')[0].get('value')

                        time.sleep(2)
                        
                        for row in root.cssselect('#ntabs-1 tr'):
                                for cell in row.cssselect('td:nth-child(3)'):
                                        if cell.text_content() == 'KBS':
                                                curs.execute(sql,(
                                                    row.cssselect('td:nth-child(4)')[0].text_content(), #title
                                                    row.cssselect('a')[1].get('href'), #url
                                                    dt, #date
                                                    row.cssselect('td:nth-child(2)')[0].text_content(), #category
                                                    int(row.cssselect('td:nth-child(1)')[0].text_content()), #rank
                                                    int(row.cssselect('td:nth-child(5)')[0].text_content().replace(',',''))  #pv
                                                    #row.cssselect('td:nth-child(5)')[0].text_content()  #pv
                                                ))
                conn.commit()

                with conn.cursor() as curs:
                        sql = "select * from naver_article order by dt desc"
                        curs.execute(sql)
                        rs = curs.fetchall()
                        for row in rs:
                                print(row)

        finally:
                conn.close()

if __name__ == '__main__':
        main()
