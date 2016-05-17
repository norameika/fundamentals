#!/usr/local/bin/python 
# -*- coding: utf-8 -*- 

print "Content-Type: text/html\n" 

import sqlite3
import cgi

form = cgi.FieldStorage()
val0, val1 = form['col0'].value, form['col1'].value


def fetch_stock(val0, val1):
    con = sqlite3.connect('./db/db.db')
    con.text_factory = str
    c = con.cursor()
    sql = """
    select * from stock where Code = '%s'
    """ % (val0)
    c.execute(sql)
    for row in c:
        print ','.join(row)
        print '\n'


fetch_stock(val0, val1)
