#!/usr/local/bin/python 
# -*- coding: utf-8 -*- 

print "Content-Type: text/html\n" 

import os
import sqlite3


def recordline_stock(con, date, code, market, name, industry, open, close, low, high, to, tv):
    con.text_factory = str
    sql = """
    insert into stock values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    con.execute(sql, (date, code, market, name, industry, open, close, low, high, to, tv))


def record_stock():
    con = sqlite3.connect('./db/db.db')
    for f in os.listdir('./data/stock'):
        date = f.split('.')[0]
        f_ = open('./data/stock/%s' % f, 'r')
        for l in f_:
            if len(l.split(',')) == 1: continue
            elif len(l.split(',')) == 10:
                code, market, name, industry, open_, close, low, high, to, tv = l.strip('\n').split(',')
                recordline_stock(con, date, code, market, name, industry, open_, close, low, high, to, tv)
            elif len(l.split(',')) == 9:
                code, market, name, open_, close, low, high, to, tv = l.strip('\n').split(',')
                recordline_stock(con, date, code, market, name, None, open_, close, low, high, to, tv)
    con.commit()
    con.rollback()

record_stock()
