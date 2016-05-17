#!/usr/local/bin/python 
# -*- coding: utf-8 -*- 

print "Content-Type: text/html\n" 

import sqlite3



def indexing():
    con = sqlite3.connect('./db/db.db')
    c = con.cursor()
    sql = """
    CREATE INDEX index_fundametals on fundamentals(EdinetID, DateOrdinary);
    """
    c.execute(sql)
    con.commit()
    con.rollback()
    print 'Done'

indexing()