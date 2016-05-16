#!/usr/local/bin/python 
# -*- coding: utf-8 -*- 

print "Content-Type: text/html\n" 

import os
import sqlite3
import shutil
import datetime

def recordline_fundamentals(con, date, date_ordinary, edinet_id, term, value, context, type, period_or_instant, is_consolidated, src):
    con.text_factory = str
    sql = """
    insert into fundamentals values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    con.execute(sql, (date, date_ordinary, edinet_id, term, value, context, type, period_or_instant, is_consolidated, src))


def record_fundamentals():
    con = sqlite3.connect('./db/db.db')
    for f in os.listdir('./db/temp'):
        f_ = open('./db/temp/%s' % f, 'r')
        for l in f_:
            print len(l.strip('\n').split(','))
            if len(l.split(',')) !=11: continue
            index, date, date_ordinary, edinet_id, term, value, context, type, period_or_instant, is_consolidated, src = l.strip('\n').split(',')
            recordline_fundamentals(con, date, date_ordinary, edinet_id, term, value, context, type, period_or_instant, is_consolidated, src)
        os.remove('./db/temp/%s' % f)
    con.commit()
    con.rollback()
    shutil.rmtree('./db/temp/')
    os.makedirs('./db/temp')
    print "done"

record_fundamentals()