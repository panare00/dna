import os
import requests
import json
import sys
import time
import pytz
from datetime import datetime, timedelta
from importlib import reload
import pymysql

'''
reloading sys for utf8 encoding is for Python 2.7
This line should be removed for Python 3
In Python 3, we need to specify encoding when open a file
f = open("file.csv", encoding='utf-8')
'''
if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding('utf8')

YESTERDAY_DATE = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

class FacebookScraper:
    '''
    FacebookScraper class to scrape facebook info
    '''
    def __init__(self, token):
        self.token = token

    @staticmethod
    def convert_to_epochtime(date_string):
        '''Enter date_string in 2000-01-01 format and convert to epochtime'''
        try:
            epoch = int(time.mktime(time.strptime(date_string, '%Y-%m-%d')))
            return epoch
        except ValueError:
            print('Invalid string format. Make sure to use %Y-%m-%d')
            quit()

    def get_feed_data(self, target_page, offset, fields, since, until):
        """
        This method will get the feed data
        """
        url = "https://graph.facebook.com/v2.10/{}/feed".format(target_page)
        param = dict()
        param["access_token"] = self.token
        param["limit"] = "100"
        param["offset"] = offset
        param["fields"] = fields
        param["since"] = since
        param["until"] = until

        r = requests.get(url, param)
        data = json.loads(r.text)
        print("json file has been generated")
        return data

    def convert_feed_data(self, response_json_list):
        list_all = []
        for response_json in response_json_list:
            data = response_json["data"]

            for i in range(len(data)):
                list_row = []
                row = data[i]
                id = row["id"]
                try:
                    type = row["type"]
                except KeyError:
                    type = ""
                try:
                    created_time = row["created_time"]
                    local_timezone = pytz.timezone('Asia/Seoul')
                    local_time = datetime.strptime(created_time[0:19],'%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.utc).astimezone(local_timezone)
                except KeyError:
                    local_time = ""
                try:
                    name = row["name"]
                except KeyError:
                    name = ""
                try:
                    description = row["description"]
                except KeyError:
                    description = ""
                try:
                    actions_link = row["actions"][0]["link"]
                except KeyError:
                    actions_link = ""
                try:
                    actions_name = row["actions"][0]["name"]
                except KeyError:
                    actions_name = ""
                try:
                    share_count = row["shares"]["count"]
                except KeyError:
                    share_count = 0
                try:
                    comment_count = row["comments"]["summary"]["total_count"]
                except KeyError:
                    comment_count = 0
                try:
                    like_count = row["post_reactions_by_type_total"]["data"][0]["values"][0]["value"]["like"]
                except KeyError:
                    like_count = 0
                try:
                    love_count = row["post_reactions_by_type_total"]["data"][0]["values"][0]["value"]["love"]
                except KeyError:
                    love_count = 0
                try:
                    wow_count = row["post_reactions_by_type_total"]["data"][0]["values"][0]["value"]["wow"]
                except KeyError:
                    wow_count = 0
                try:
                    haha_count = row["post_reactions_by_type_total"]["data"][0]["values"][0]["value"]["haha"]
                except KeyError:
                    haha_count = 0
                try:
                    sorry_count = row["post_reactions_by_type_total"]["data"][0]["values"][0]["value"]["sorry"]
                except KeyError:
                    sorry_count = 0
                try:
                    anger_count = row["post_reactions_by_type_total"]["data"][0]["values"][0]["value"]["anger"]
                except KeyError:
                    anger_count = 0
                try:
                    impression = row["insights"]["data"][0]["values"][0]["value"]
                except KeyError:
                    impression = 0

                list_row.extend((id, type, local_time, name, actions_link, impression, share_count, comment_count, like_count, love_count, wow_count, haha_count, sorry_count, anger_count))
                list_all.append(list_row)

        return list_all

    def insert_table(self, feed_table_list):
        conn = pymysql.connect(host='localhost', user='scraper', password='tiger', db='scraping', charset='utf8')
        try:
            #with conn.cursor() as curs:
            #    sql = "delete from facebook_feed where dt >= %s"
            #    curs.execute(sql,(YESTERDAY_DATE))
            #conn.commit()
            with conn.cursor() as curs:
                sql = """insert into facebook_feed(id, type, dt, name, url, impression, share_count, comment_count, like_count, love_count, wow_count, haha_count, sorry_count, anger_count) value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                for feed_list in feed_table_list:
                    curs.execute(sql,(
                        feed_list[0], #id
                        feed_list[1], #type
                        feed_list[2].isoformat()[:10], #dt
                        feed_list[3], #name
                        feed_list[4], #url
                        int(feed_list[5]), #impression
                        int(feed_list[6]), #share_count
                        int(feed_list[7]), #comment_count
                        int(feed_list[8]), #like_count
                        int(feed_list[9]), #love_count
                        int(feed_list[10]), #wow_count
                        int(feed_list[11]), #haha_count
                        int(feed_list[12]), #sorry_count
                        int(feed_list[13]) #anger_count
                    ))
            conn.commit()
            with conn.cursor() as curs:
                sql = "select * from facebook_feed order by dt desc"
                curs.execute(sql)
                rs = curs.fetchall()
                for row in rs:
                    print(row)
        except Error as error:
            print(error)
        finally:
            conn.close()

def scrape_job(target_page_input, token_input, field_input, since, until):

    fb = FacebookScraper(token_input)
    offset = 0
    json_list = []
    while True:
        try:
            data = fb.get_feed_data(target_page_input, str(offset), field_input, since, until)
            check = data['data']
            if (len(check) >= 100):
                json_list.append(data)
                offset += 100
            else:
                json_list.append(data)
                print("End of loop for obtaining more than 100 feed records.")
                break
        except KeyError:
            print("Error with get request.")
            quit()

    feed_table_list = fb.convert_feed_data(json_list)
    fb.insert_table(feed_table_list)


if __name__ == "__main__":

    since = YESTERDAY_DATE + 'T00:00:00+09:00'
    until = YESTERDAY_DATE + 'T23:59:59+09:00'

    field_input = 'id,created_time,name,shares,type,published,link,likes.summary(true),actions,description,insights.metric(post_reactions_by_type_total).period(lifetime).as(post_reactions_by_type_total),insights.metric(post_impressions).period(lifetime)'

    #kbs news 
    kbsnews_pageid = "121798157879172" 
    kbsnews_token = os.environ['KBSNEWS_API_KEY']
    scrape_job(kbsnews_pageid, kbsnews_token, field_input, since, until)

    #keyah
    kbsyah_pageid = "985806658205900"
    kbsyah_token = os.environ['KBSYAH_API_KEY']
    scrape_job(kbsyah_pageid, kbsyah_token, field_input, since, until)
