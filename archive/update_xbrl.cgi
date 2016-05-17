#!/usr/local/bin/python 
# -*- coding: utf-8 -*- 

print "Content-Type: text/html\n" 

import os
import sqlite3
import shutil
import datetime


def recordline_xbrl(con, date, date_ordinary, edinet_id, term, value, context, type, period_or_instant, is_consolidated, src):
    con.text_factory = str
    sql = """
    insert into stock values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    con.execute(sql, (date, date_ordinary, edinet_id, term, value, context, type, period_or_instant, is_consolidated, src))


def record_xbrl():
    con = sqlite3.connect('./db/db.db')
    for f in os.listdir('./db/temp'):
        date = f.split('.')[0]
        f_ = open('./db/temp/%s' % f, 'r')
        for l in f_:
            if len(l.split(',')) == 1: continue
            elif len(l.split(',')) == 19:
                date, date_ordinary, edinet_id, term, value, context, type, period_or_instant, is_consolidated, src = l.strip('\n').split(',')
                recordline_xbrl(con, date, date_ordinary, edinet_id, term, value, context, type, period_or_instant, is_consolidated, src)
        os.remove('./db/temp/%s' % f)
    con.commit()
    con.rollback()
    shutil.rmtree('./db/temp/')
    os.makedirs('./db/temp')
    print "done"

record_stock()
