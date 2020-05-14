# coding=utf8

import requests
import time
import json
from datetime import datetime
import time
import pandas as pd
import urllib.parse
import sys


class TrendsByDay:
    GENERAL_URL = 'https://trends.google.com/trends/api/explore'
    TRENDS_BY_REGION_URL = 'https://trends.google.com/trends/api/widgetdata/comparedgeo'
    def __init__(self, kw_list, startdate, days):
        # parameters
        self.tz = 240
        self.hl = 'en-US'
        self.geo  = 'US'
        self.proxy = ''
        self.timeout = 2

        self.dates = [startdate]
        self.kw_list = kw_list
        self.build_dates()
        
        # preparing payloads
        self.cookies = dict()
        self.token_payload = dict()
        self.interest_by_region_widget = dict()
        self.timeframe = '2020-01-20 2020-01-21'
        self.cookies = self.get_cookies()

        
        self.build_token_payload()
        # need to recall this func to rebuild token payload for every timeframe

    def build_dates(self):
        year, month, day = startdate.split('-')
        bigmonths = [1,3,5,7,8,10,12]
        littlemonths = [4,6,9,11]
        for i in range(days):
            if int(month) in bigmonths and int(day)+1 > 31 \
                or int(month) in littlemonths and int(day)+1 > 30 \
                or int(month) == 2 and int(day)+1 > 29:
                day = '01'
                if month == '12':
                    month = '01'
                    year = str(int(year)+1)
                else:
                    month = '%02d' % (int(month)+1)
            else:
                day = '%02d' % (int(day)+1)
            self.dates += year+'-'+month+'-'+day,

    def build_token_payload(self, time='2020-01-20 2020-01-21'):
        self.timeframe  = time
        self.token_payload =  {
            'hl': self.hl,
            'tz': self.tz,
            'req': {'comparisonItem': [], 'category': 0, 'property': ''}
        }
        for kw in self.kw_list:
            keyword_payload = {'keyword': kw, 'time': self.timeframe,
                               'geo': self.geo}
            self.token_payload['req']['comparisonItem'].append(keyword_payload)

        self.token_payload['req'] = json.dumps(self.token_payload['req'])
        self.fetch_tokens()


    def get_cookies(self):
        while True:
            try:
                return dict(filter(lambda i: i[0] == 'NID', requests.get(
                    'https://trends.google.com/?geo={geo}'.format(
                        geo=self.geo),
                    timeout=self.timeout,
                    proxies=self.proxy
                ).cookies.items()))
            except Exception as e:
                print(e)
                return

    def fetch_data(self, url, method='get', trim_chars=0, **kwargs):
        s = requests.session()
        # print(url)
        s.headers.update({'accept-language': self.hl})
        if method == 'post':
            response = s.post(url, timeout=timeout,
                                cookies=self.cookies, **kwargs)
        else:
            response = s.get(url, timeout=self.timeout, cookies=self.cookies,
                                **kwargs)
        if response.status_code == 200:
            content = response.text[trim_chars:]
            return json.loads(content)
        else:
            # error
            print("response error")
            return None

    def fetch_tokens(self):
        
        widget_dict = self.fetch_data(
            url=TrendsByDay.GENERAL_URL,
            method='get',
            params=self.token_payload,
            trim_chars=4,
        )['widgets']

        for widget in widget_dict:
            if widget['id'] == 'GEO_MAP':
                self.interest_by_region_widget = widget
                return

    def trends_by_region(self, inc_low_vol=False, inc_geo_code=True):
        region_payload = dict()
        region_payload['req'] = json.dumps(self.interest_by_region_widget['request'])
        region_payload['token'] = self.interest_by_region_widget['token']
        region_payload['tz'] = self.tz

        # parse returned json
        req_json = self.fetch_data(
            url=TrendsByDay.TRENDS_BY_REGION_URL,
            method='get',
            trim_chars=5,
            params=region_payload,
        )

        df = pd.DataFrame(req_json['default']['geoMapData'])
        if (df.empty):
            return df

        df['date'] = '/'.join(self.timeframe.split(' ')[0].split('-'))

        # index the df and sort by 'geoCode'
        df = df[['date', 'geoCode', 'value']].set_index('date').sort_values('geoCode')

        # wash 'value' column
        result_df = df['value'].apply(lambda x: pd.Series(
            str(x).replace('[', '').replace(']', '').split(',')))
        
        result_df['geoCode'] = df['geoCode']


        for idx, kw in enumerate(self.kw_list):
            time.sleep(1)
            result_df[kw] = result_df[idx].astype('int')
            del result_df[idx]
        result_df.reset_index()
        return result_df

    def trends_by_region_and_day(self, inc_low_vol=False, inc_geo_code=True, save_path='trends_by_day123.csv'):
        with open(save_path, 'w') as fp:
            fp.write('date,geoCode')
            for kw in self.kw_list:
                fp.write(','+kw)
            fp.write('\n')
        # exit(0)
        for i in range(len(self.dates)-1):
            startday = self.dates[i]
            endday = self.dates[i+1]
            self.build_token_payload(startday+' '+endday)

            df = self.trends_by_region(inc_low_vol, inc_geo_code)

            # write into save_path as a csv file
            df.to_csv(save_path, mode='a', header=False)


def read_kw_from_file(fn, kw_list):
    with open(fn, 'r') as fp:
        lines = fp.readlines()
        for line in lines:
            kw_list += line.strip('\n'),


if __name__ == "__main__":

    # start date
    startdate = '2020-01-20'
    days = 30                   # time frame by day
    save_path = 'trends_by_day.csv'
    
    if len(sys.argv) > 1:
        startdate = sys.argv[1]
    if len(sys.argv) > 2:
        days = int(sys.argv[2])
    if len(sys.argv) > 3:
        save_path = sys.argv[3]

    kw_fn = 'C:\\Users\\ED\\Documents\\covid\\data_classfication-19\\Code\\keywords'          # filename of keywords list
    # kw_list = ['mask']
    kw_list = []
    read_kw_from_file(kw_fn, kw_list)

    print("Collecting trends data starting from {startdate}, duration {days}d.\n" \
        .format(startdate=startdate, days=days),\
        "Target keywords: ", kw_list, '\n' \
        "Writing into file: {save_path} ...".format(save_path=save_path))


    test = TrendsByDay(kw_list, startdate, days)
    test.trends_by_region_and_day(save_path=save_path)

