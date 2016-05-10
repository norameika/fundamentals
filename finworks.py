# coding:utf-8

# Script Name   : financials.py
# Author        : Yoshi Yonai
# Created       : 2016 0508
# Last Modified : 
# Version       : 1.0
# Modifications : 

# Description   : financial findamentals analysis

from xbrl import XBRLParser
from urllib2 import HTTPError
import urllib
import urllib2
import datetime
import pandas as pd
import os
import zipfile
import pysftp
import re
import shutil
pd.set_option('display.width', 1000)

UPLOAD_PATH = '/home/yoshiharuyonai/www/karasu/db/temp'
HOST = 'norameika.com'
USER = ''
PASSWORD = ''

# class FinWorks():
#     def __init__(self, edinet_id):
#         self.edinet_id = edinet_id
#         self.id


class XbrlParser(XBRLParser):
    def __init__(self, fp, src):
        self.fp = fp
        self.src = src
        self.contexts = dict()
        self.set_contexts()
        self.id = self.set_id()

    def set_id(self):
        xbrl = XBRLParser.parse(open(self.fp, 'r'))
        namespace = 'xbrli:identifier'
        return xbrl.find(name=re.compile(namespace)).string.split('-')[0]

    def set_contexts(self):
        xbrl = XBRLParser.parse(open(self.fp, 'r'))
        namespace = 'xbrli:context'
        for node in xbrl.find_all(name=re.compile(namespace)):
            if 'NonConsolidated' in node.attrs['id']:
                consoli = 0
            elif 'Consolidated' in node.attrs['id']:
                consoli = 1
            else:
                consoli = 'na'
            if 'Duration' in node.attrs['id']:
                if not node.find('xbrli:startdate'): continue
                if not node.find('xbrli:enddate'): continue
                sdate = node.find('xbrli:startdate').string
                edate = node.find('xbrli:enddate').string
                self.contexts.update({node.attrs['id']: ['Duration', '%s:%s' % (sdate, edate), consoli]})
            elif 'Instant' in node.attrs['id']:
                if not node.find('xbrli:instant'): continue
                self.contexts.update({node.attrs['id']: ['Instant', node.find('xbrli:instant').string, consoli]})

    def parse_xbrl(self, fp):
        # parse xbrl file
        out = list()
        xbrl = XBRLParser.parse(open(self.fp, 'r'))
        name_space = 'jp*'
        for node in xbrl.find_all(name=re.compile(name_space + ':*')):
            if not node.string: continue
            if not node.string.isdigit(): continue
            out.append([self.id, node.name.split(':')[-1], node.string] + [node.attrs['contextref']] + self.contexts[node.attrs['contextref']] + [self.src])
        pd.DataFrame(out, columns=['EdinetID', 'Term', 'Value', 'Context', 'Type', 'PeriodOrInstant', 'IsConsolidated', 'Src']).to_csv(fp)


def update_xbrl(edinet_id):
    sftp = pysftp.Connection(HOST, username=USER, password=PASSWORD)
    sftp.chdir(UPLOAD_PATH)
    xbrllink_download(edinet_id)
    df = pd.read_csv('./data/xbrl_download_hist.csv', index_col=0)
    for [edinet_id, identification, is_downloaded, datatime, url] in df.values:
        if not is_downloaded:
            download_file(url, './data/temp')
            print url, "downloaded"
    for root, dirs, files in os.walk('./data/temp'):
        for file in files:
            if '.xbrl' in file and 'AuditDoc' not in root:
                fp = root + '/' + file
                print fp
                xbrl = XbrlParser(fp, url)
                xbrl.parse_xbrl('./data/temp/%s.csv' % file.split('.')[0])
                # sftp.put('./data/temp/%s' % file)
    # shutil.rmtree('./data/temp')
    sftp.close()


def xbrllink_download(edinet_id):
    url = 'http://www.someserver.com/cgi-bin/register.cgi'
    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.63 Safari/537.36'
    headers = {'User-Agent': user_agent}
    url = r'https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp?uji.verb=W1E63011CXW1E6A011DSPSch&uji.bean=ee.bean.parent.EECommonSearchBean&TID=W1E63011&PID=W1E63011&SESSIONKEY=1423664511179&lgKbn=2&pkbn=0&skbn=1&dskb=&askb=&dflg=0&iflg=0&preId=1&mul=%s&fls=on&cal=1&era=H&yer=&mon=&pfs=5&row=100&idx=0&str=&kbn=1&flg=&syoruiKanriNo=' % edinet_id
    req = urllib2.Request(url, None, headers)
    df = pd.read_csv('./data/xbrl_download_hist.csv', index_col=0)
    try:
        html = urllib2.urlopen(req).read()
    except HTTPError:
        print 'failed'
        return 0
    for l in html.split('\n'):
        if 'javascript:downloadFile' in l:
            query = l.split('downloadFile(')[-1].split(');">')[0].split(',')
            query = [i.replace("'", "").strip() for i in query]
            query = [i for i in query if 'null' not in i][1:]
            identification = re.split('=|&', query[1])[1]
            url = 'https://disclosure.edinet-fsa.go.jp/E01EW/download?uji.verb=W0EZA104CXP001006BLogic&uji.bean=%s' % ('&'.join(query))
            if identification in df['Identification'].values:
                print edinet_id, identification, 'is already exsiting'
                continue
            df_add = pd.DataFrame([[edinet_id, identification, 0, 'na', url]], columns=['EdinetID', 'Identification', 'IsDownloaded', 'Datetime', 'URL'])
            df = pd.concat([df, df_add], axis=0, ignore_index=True)
            print edinet_id, identification, 'is added'
    df.to_csv('./data/xbrl_download_hist.csv')


def fetch_stock(stock_code, date):
    date_ord = datetime.date(date[0], date[1], date[2]).toordinal()
    url = 'http://norameika.com/karasu/fetch_stock.cgi?col0=%s&col1=%s' % (stock_code, date_ord)
    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.63 Safari/537.36'
    headers = {'User-Agent': user_agent}
    req = urllib2.Request(url, None, headers)
    try:
        html = urllib2.urlopen(req)
    except HTTPError:
        print 'failed'
    data = [i.strip('\n').split(',') for i in html.readlines()]
    df = pd.DataFrame(data, columns=['Date', 'DateOrdinary', 'Code', 'Market', 'Name', 'Industry', 'Open', 'Close', 'Low', 'High', 'Turnover', 'Trading_volume'])
    df = df.sort(columns=['DateOrdinary', ]).drop_duplicates(subset='DateOrdinary')
    return df


def update_stock():
    sftp = pysftp.Connection(HOST, username=USER, password=PASSWORD)
    sftp.chdir(UPLOAD_PATH)
    date = datetime.date(2010, 1, 1)
    cnt = 0
    while not date == datetime.date.today() + datetime.timedelta(days=1):
        print date,
        if not'%s.csv' % str(date) in os.listdir('./data/stock'):
            query = 'http://k-db.com/stocks/%s?download=csv' % str(date)
            urllib.urlretrieve(query, './data/stock/%s.csv' % str(date))
            sftp.put('./data/stock/%s.csv' % str(date))
            print 'done'
            cnt += 1
        else:
            print 'skip'
        if cnt == 10:
            url = 'http://norameika.com/karasu/update_stock.cgi'
            user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.63 Safari/537.36'
            headers = {'User-Agent': user_agent}
            req = urllib2.Request(url, None, headers)
            try:
                urllib2.urlopen(req)
                print 'intgrated to db by', date
            except HTTPError:
                print 'failed'
            cnt = 0
        date += datetime.timedelta(days=1)
    sftp.close()

    url = 'http://norameika.com/karasu/update_stock.cgi'
    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.63 Safari/537.36'
    headers = {'User-Agent': user_agent}
    req = urllib2.Request(url, None, headers)
    try:
        urllib2.urlopen(req)
    except HTTPError:
        print 'failed'


def unzip(src, fp):
    zf = zipfile.ZipFile(src, 'r')
    for f in zf.namelist():
        if not os.path.exists(fp + '/' + os.path.dirname(f)):
            os.makedirs(fp + '/' + os.path.dirname(f))
        uzf = file(fp + '/' + f, 'w')
        uzf.write(zf.read(f))
        uzf.close()


def download_file(url, fp):
    if not os.path.exists(fp):
        os.makedirs(fp)
    urllib.urlretrieve(url, './data/temp.zip')
    unzip('./data/temp.zip', fp)

if __name__ == '__main__':
    # update_stock()
    # print fetch_stock('1721-T', (2013, 1, 1))
    update_xbrl('E00097')


