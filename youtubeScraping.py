import os
import pymysql
import sys
import dateutil.parser
import pytz
from apiclient.discovery import build
from datetime import datetime, timedelta

API_KEY = os.environ['YOUTUBE_API_KEY']
YESTERDAY_DATE = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

def convert_feed_data():
    list_all = []

    for items_per_page in search_videos():
        for item in items_per_page:
            list_row = []
            try:
                title = item['snippet']['title']
            except KeyError:
                title = ""
            try:
                thumbnails = item['snippet']['thumbnails']['default']['url']
            except KeyError:
                thumbnails = ""
            try:
                url = "https://www.youtube.com/watch?v=" + item['id']
            except KeyError:
                url = ""
            try:
                date = dateutil.parser.parse(item['snippet']['publishedAt'])
                local_timezone = pytz.timezone('Asia/Seoul')
                local_date = date.replace(tzinfo=pytz.utc).astimezone(local_timezone)
                dt = local_date.isoformat()[:10]
            except KeyError:
                dt = ""
            try:
                publish_time = local_date.isoformat()
            except KeyError:
                publish_tiem = ""
            try:
                viewCount = int(item['statistics']['viewCount'])
            except KeyError:
                viewCount = 0
            try:
                likeCount = int(item['statistics']['likeCount'])
            except KeyError:
                likeCount = 0
            try:
                commentCount = int(item['statistics']['commentCount'])
            except KeyError:
                commentCount = 0
            try: 
                liveBroadcastContent = item['snippet']['liveBroadcastContent']
            except KeyError:
                liveBroadcastContent = ""

            list_row.extend((title, thumbnails, url , dt, publish_time, viewCount, likeCount, commentCount, liveBroadcastContent))
            print(list_row)
            list_all.append(list_row)

    return list_all

def insert_table(feed_table_list):
    conn = pymysql.connect(host='localhost', user='scraper', password='tiger', db='scraping', charset='utf8')
    try:
            with conn.cursor() as curs:
                    sql = "delete from youtube_article where dt = %s"
                    curs.execute(sql,(YESTERDAY_DATE))
            conn.commit()
            with conn.cursor() as curs:
                    sql = """insert into youtube_article(title, thumbnails, url, dt, publishedTime, viewCount, likeCount, commentCount ) values (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    for feed_list in feed_table_list:
                        if feed_list[8] == 'none':
                            curs.execute(sql,(
                                feed_list[0],
                                feed_list[1],
                                feed_list[2],
                                feed_list[3],
                                feed_list[4],
                                feed_list[5],
                                feed_list[6],
                                feed_list[7]
                            ))
                    conn.commit()
                    with conn.cursor() as curs:
                        sql = "select dt, count(*) from youtube_article group by dt"
                        curs.execute(sql)
                        rs = curs.fetchall()
                        for row in rs:
                            print(row)
    except Error as error:
        print(error)
    finally:
        conn.close()

def search_videos(max_pages=5):
        youtube = build('youtube', 'v3', developerKey=API_KEY)
        search_request = youtube.search().list(
            part = 'id',
            channelId = 'UCcQTRi69dsVYHN3exePtZ1A',
            order = 'date',
            #publishedAfter = '2018-06-23T00:00:00+09:00',
            publishedAfter = YESTERDAY_DATE + 'T00:00:00+09:00',
            regionCode = 'KR',
            maxResults = 50,
        )
        i = 0
        while search_request and i < max_pages:
            search_response = search_request.execute()
            #video_ids = [item["id"]["videoId"] for item in search_response["items"]]
            video_ids = [item['id'].get('videoId') for item in search_response['items']]
            videos_response = youtube.videos().list(
                part='snippet,statistics',
                id = ','.join(filter(None, video_ids))
            ).execute()
            yield videos_response['items']

            search_request = youtube.search().list_next(search_request, search_response)
            i += 1

if __name__ == '__main__':
    feed_table_list = convert_feed_data()
    insert_table(feed_table_list)

