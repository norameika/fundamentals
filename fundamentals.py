# coding:utf-8

# Script Name   : fundamentals.py
# Author        : Yoshi Yonai
# Created       : 2016 0508
# Last Modified : 2016 0515
# Modifications : 

# Description   : financial findamentals analysis

from bs4 import BeautifulSoup
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
import pylab as plt
pd.set_option('display.width', 1000)

UPLOAD_PATH = '/home/yoshiharuyonai/www/karasu/db/temp'
HOST = 'norameika.com'
USER = ''
PASSWORD = ''


class Fundamentals():
    def __init__(self, df, chart_type='line'):
        self.df = df.sort(columns='DateOrdinary')
        self.legend = df.columns[2:]
        self.chart_type = chart_type

    def visualize(self):
        c = chart()
        data = [[self.df.ix[:, 1].values, self.df.ix[:, col].values] for col in self.legend]
        if self.chart_type == 'line':
            c.scatter(data, legend=self.legend)
        elif self.chart_type == 'bar':
            c.bar(data, legend=self.legend)
        c.draw()


class CorpFundamental(Fundamentals):
    def __init__(self, edinet_id):
        self.edinet_id = edinet_id
        self.is_listed, self.is_consolidated, self.name, self.secur_code = self.set_parameters()
        self.is_listed = int(self.is_listed is 'Listed company')
        self.is_consolidated = int(self.is_consolidated is 'Consolidated')
        self.secur_code = str(int(self.secur_code))[:-1] + '-T'
        Fundamentals.__init__(self, self.fetch_stock())

    def set_parameters(self):
        df = pd.read_csv('./id_list/EdinetcodeDlInfo.csv', skiprows=1)
        return df[df['EDINET Code'] == self.edinet_id].ix[:, ['Listed company / Unlisted company',
                                                              'Consolidated / NonConsolidated',
                                                              'Submitter Name',
                                                              'Securities Identification Code']].values[0]

    def fetch_stock(self):
        return fetch_stock(self.secur_code, (2010, 1, 1)).ix[:, ['Date', 'DateOrdinary', 'Close', 'Low', 'High']]

    def fetch_fundamental(self):
        terms = pd.read_csv('./data/terms.csv').ix[:, ['Name', 'UniqueName']]

    def describe(self):
        print self.edinet_id, self.secur_code, self.is_listed, self.is_consolidated


class XbrlParser():
    def __init__(self, fp, src):
        self.fp = fp
        self.src = src
        self.contexts = dict()
        self.set_contexts()
        self.id = self.set_id()

    def set_id(self):
        xbrl = self.parse(self.fp)
        namespace = 'xbrli:identifier'
        return xbrl.find(name=re.compile(namespace)).string.split('-')[0]

    def set_contexts(self):
        xbrl = self.parse(self.fp)
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
                self.contexts.update({node.attrs['id']: ['Instant', '%s' % node.find('xbrli:instant').string, consoli]})

    def parse_xbrl(self, fp):
        # parse xbrl file
        out = list()
        xbrl = self.parse(self.fp)
        name_space = 'jp*'
        for node in xbrl.find_all(name=re.compile(name_space + ':*')):
            if node.string is None: continue
            if not node.string.isdigit(): continue
            if node.attrs['contextref'] not in self.contexts.keys(): continue
            date = self.contexts[node.attrs['contextref']][1].split(':')[-1]
            y, m, d = map(int, date.split('-'))
            date_ord = datetime.date.toordinal(datetime.date(y, m, d))
            out.append([date, date_ord, self.id, node.name.split(':')[-1], node.string] + [node.attrs['contextref']] + self.contexts[node.attrs['contextref']] + [self.src])
        if len(out) > 0:
            pd.DataFrame(out, columns=['Date', 'DateOrdinary', 'EdinetID', 'Term', 'Value', 'Context', 'Type', 'PeriodOrInstant', 'IsConsolidated', 'Src']).to_csv(fp)
            return 1
        else:
            print fp, 'failed'
            return 0

    def parse(self, fp):
        soup = BeautifulSoup(open(fp, 'r'), 'html.parser')
        return soup


def update_xbrl(edinet_id):
    sftp = pysftp.Connection(HOST, username=USER, password=PASSWORD)
    sftp.chdir(UPLOAD_PATH)
    xbrllink_download(edinet_id)
    urlmap = dict()
    df = pd.read_csv('./data/xbrl_download_hist.csv', index_col=0)
    for cnt, [edinet_id, identification, is_downloaded, datatime, url] in enumerate(df.ix[:, ['EdinetID',
                                                                                              'Identification',
                                                                                              'IsDownloaded',
                                                                                              'Datatime',
                                                                                              'URL']].values):
        if not is_downloaded:
            fnames = download_file(url, './data/temp')
            print url, "downloaded"
            for f in fnames:
                urlmap.update({f: url})
            df.ix[cnt, 'IsDownloaded'] = 1
    for root, dirs, files in os.walk('./data/temp'):
        for file in files:
            if '.xbrl' in file and 'AuditDoc' not in root:
                fp = root + '/' + file
                xbrl = XbrlParser(fp, urlmap[re.split('/|\\\\', root)[3]])
                print fp,
                if_succeed = xbrl.parse_xbrl('./data/temp/%s.csv' % file.split('.')[0])
                if if_succeed:
                    sftp.put('./data/temp/%s.csv' % file.split('.')[0])
                    url = 'http://norameika.com/karasu/update_fundamentals.cgi'
                    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.63 Safari/537.36'
                    headers = {'User-Agent': user_agent}
                    req = urllib2.Request(url, None, headers)
                    try:
                        urllib2.urlopen(req)
                        print 'intgrated to db',
                    except HTTPError:
                        print 'did not integrated to db',
                    print 'done'
    if os.path.exists('./data/temp'): shutil.rmtree('./data/temp')
    sftp.close()
    df.to_csv('./data/xbrl_download_hist.csv')


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


def fetch_fundamentals(edinet_id, date):
    date_ord = datetime.date(date[0], date[1], date[2]).toordinal()
    url = 'http://norameika.com/karasu/fetch_fundamentals.cgi?col0=%s&col1=%s' % (edinet_id, date_ord)
    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.63 Safari/537.36'
    headers = {'User-Agent': user_agent}
    req = urllib2.Request(url, None, headers)
    try:
        html = urllib2.urlopen(req)
    except HTTPError:
        print 'failed'
        return 0, 0
    data = [i.strip('\n').strip('\r').split(',') for i in html.readlines()]
    if len(data) == 0:
        print edinet_id, 'failed'
        return 0, 0
    df = pd.DataFrame(data, columns=['Date', 'DateOrdinary', 'EdinetID', 'Term', 'Value', 'Context', 'Type', 'PeriodorInstant', 'IsConsolidated', 'Src'])
    df = df.sort(columns=['DateOrdinary', ])
    return 1, df


def fetch_stock(stock_code, date):
    date_ord = datetime.date(date[0], date[1], date[2]).toordinalrdinal()
    url = 'http://norameika.com/karasu/fetch_stock.cgi?col0=%s&col1=%s' % (stock_code, date_ord)
    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.63 Safari/537.36'
    headers = {'User-Agent': user_agent}
    req = urllib2.Request(url, None, headers)
    try:
        html = urllib2.urlopen(req)
    except HTTPError:
        print 'failed'
        return 0, 0
    data = [i.strip('\n').strip('\r').split(',') for i in html.readlines()]
    df = pd.DataFrame(data, columns=['Date', 'DateOrdinary', 'Code', 'Market', 'Name', 'Industry', 'Open', 'Close', 'Low', 'High', 'Turnover', 'TradingVolume'])
    df = df.sort(columns=['DateOrdinary', ]).drop_duplicates(subset='DateOrdinary')
    return 1, df


def update_stock():
    sftp = pysftp.Connection(HOST, username=USER, password=PASSWORD)
    sftp.chdir(UPLOAD_PATH)
    date = datetime.date(2016, 1, 1)
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
    out = list()
    for f in zf.namelist():
        if not os.path.exists(fp + '/' + os.path.dirname(f)):
            os.makedirs(fp + '/' + os.path.dirname(f))
        uzf = file(fp + '/' + f, 'w')
        uzf.write(zf.read(f))
        uzf.close()
        out.append(f)
    return set([i.split('/')[0] for i in out])


def download_file(url, fp):
    if not os.path.exists(fp):
        os.makedirs(fp)
    urllib.urlretrieve(url, './data/temp.zip')
    return unzip('./data/temp.zip', fp)


class chart:
    def __init__(self, x=1, y=1, ws=0.1, **kwargs):
        self.colors = ('#2980b9', '#e74c3c', '#27ae60', '#f1c40f', '#8e44ad', '#e67e22', '#bc8f8f', '#2c3e50')
        if 'aspect_ratio' in kwargs.keys():
            aspect_ratio = kwargs['aspect']
        else:
            aspect_ratio = (16, 9)
        self.fig = plt.figure(figsize=aspect_ratio, dpi=100, facecolor='w', edgecolor='k')
        self.axis = list()
        self.x = x
        self.y = y

        for i in range(x * y):
            if 'd3' in kwargs.keys():
                if i in kwargs['d3']:
                    self.axis.append(self.fig.add_subplot(y, x, i + 1, axisbg='white', projection='3d'))
            else:
                self.axis.append(self.fig.add_subplot(y, x, i + 1, axisbg='white'))
            self.fig.subplots_adjust(wspace=0.2)
            self.fig.subplots_adjust(hspace=0.4)

    def set_xlim(self, xrange, axisid=0):
        axis = self.axis[axisid]
        axis.set_xlim(xrange)

    def set_ylim(self, yrange, axisid=0):
        axis = self.axis[axisid]
        axis.set_ylim(xrange)

    def bar_chart(self, data, lw=0.77, alpha=1, *args, **kwargs):
        self.data.update({axis_id: data})
        ax = self.axis[axis_id]
        colors = self.colors
        buf = None
        if 'twinx' in kwargs.keys():
            ax = ax.twinx()
        if 'fs' in kwargs.keys():
            fs = kwargs['fs']
        else:
            fs = 13
        if 'title' in kwargs.keys():
            ax.set_title(kwargs['title'])
        if 'xlabel' in kwargs.keys():
            ax.set_xlabel(kwargs['xlabel'])
        if 'ylabel' in kwargs.keys():
            ax.set_ylabel(kwargs['ylabel'])

        if 'legend' not in kwargs.keys():
            label = range(len(data))
        else:
            label = kwargs['legend']

        for cnt, (d, c, l) in enumerate(zip(data, colors, label)):
            if cnt == 0:
                ax.bar(d[0], d[1], color=c, alpha=1, linewidth=lw, label=l)
                buf = d[1]
            else:
                ax.bar(d[0], d[1], color=c, alpha=1, linewidth=lw, bottom=buf, label=l)
                buf = (np.array(buf) + np.array(d[1])).tolist()

        if 'legend' in kwargs.keys():
            if 'loc' in kwargs.keys():
                ax.legend(fontsize=fs, loc=kwargs['loc'])
            else:
                ax.legend(fontsize=fs, loc='lower left')

        plt.legend(fontsize=fs)
        for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels()):
            item.set_fontsize(fs)
        self.canvas.draw()

    def scatter(self, data, axis_id=0, maker='-o', lw=0.77, alpha=1, *args, **kwargs):
        ax = self.axis[axis_id]
        if 'twinx' in kwargs.keys():
            ax = ax.twinx()
        if 'fs' in kwargs.keys():
            fs = kwargs['fs']
        else:
            fs = 13
        if 'title' in kwargs.keys():
            ax.set_title(kwargs['title'])
        if 'xlabel' in kwargs.keys():
            ax.set_xlabel(kwargs['xlabel'])
        if 'ylabel' in kwargs.keys():
            ax.set_ylabel(kwargs['ylabel'])

        if 'legend' not in kwargs.keys():
            label = range(len(data))
        else:
            label = kwargs['legend']
        for d, c, l in zip(data, self.colors, label):
            ax.plot(d[0], d[1], maker, ms=ms, color=c, alpha=alpha, linewidth=lw, label=l)

        if 'xtick' in kwargs.keys():
            if 'rot' in kwargs.keys():
                rot = kwargs['rot']
            else:
                rot = 0
            xtick = kwargs['xtick']
            ax.xaxis.set_ticks(data[0][0])
            ax.set_xticklabels(xtick, rotation=rot)

        if 'legend' in kwargs.keys():
            if 'loc' in kwargs.keys():
                ax.legend(fontsize=fs, loc=kwargs['loc'])
            else:
                ax.legend(fontsize=fs, loc='lower left')

        for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels()):
            item.set_fontsize(fs)

    def draw(self):
        # plt.tight_layout()
        plt.show()


def update_xbrl_all():
    df = pd.read_csv('./id_list/EdinetcodeDlInfo.csv', skiprows=1)
    df_ = pd.read_csv('./id_list/EdinetcodeDlInfo_Record_fixed_2016-05-15.csv')
    df['IsRecorded'] = pd.Series([0 for i in range(len(df))])
    print len(df), len(df['Securities Identification Code'].dropna())
    for cnt, (edinet_id, secur_id) in enumerate(df.ix[:, ['EDINET Code', 'Securities Identification Code']].values):
        print '%s / %s' % (cnt, len(df)), edinet_id, secur_id
        if secur_id == secur_id and df_[df['EDINET Code'] == edinet_id].IsRecorded.values[0] == 0:
            df.ix[cnt, 'IsRecorded'] = 1
            update_xbrl(edinet_id)
            df.ix[:, ['EDINET Code', 'Submitter Name', 'IsRecorded']].to_csv('./id_list/EdinetcodeDlInfo_Record_%s.csv' % datetime.date.today())


def test():
    import random
    df = pd.read_csv('./id_list/EdinetcodeDlInfo.csv', skiprows=1)
    df = df.dropna(subset=['Securities Identification Code'])
    for i in range(100):
        data = list()
        tar = random.choice(df['EDINET Code'].values)
        print tar
        if_succeed, df_ = fetch_fundamentals(tar, (2010, 1, 1))
        if not if_succeed: continue
        for term in set(df_.Term.values):
            data.append([term, len(df_[df_.Term == term])])
        # print pd.Series(zip(*data)[1], index=zip(*data)[0])
        if i == 0:
            out = pd.DataFrame(pd.Series(zip(*data)[1], index=zip(*data)[0]), columns=[tar])
        else:
            out[tar] = pd.Series(zip(*data)[1], index=zip(*data)[0])
    out.to_csv('terms_.csv')

if __name__ == '__main__':
    # fetch_fundamentals('E01794', (2010, 1, 1)).to_csv('E01794.csv')
    # update_xbrl_all()
    # update_stock()
    # print fetch_stock('1721-T', (2013, 1, 1))
    # update_xbrl('E00279')
    # update_xbrl'E00187')
    # CorpFundamental('E05625').visualize()
