#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import Assignment1

range_meta= 'RangeRatingsMetadata'
range_part= 'RangeRatingsPart'
round_part= 'RoundRobinRatingsMetadata'
round_meta= 'RoundRobinRatingsPart'
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur1= openconnection.cursor()
    cur2 = openconnection.cursor()
    openfile = open('RangeQueryOut.txt', 'w')
    if ratingMaxValue==0 and ratingMinValue==0:
        cur2.execute("SELECT * FROM %s WHERE maxrating>=%f and minrating<=%f" % (range_meta, ratingMinValue, ratingMaxValue))
    else:
        cur2.execute("SELECT * FROM %s WHERE maxrating>=%f and minrating<%f" %(range_meta,ratingMinValue,ratingMaxValue))
    cur1.execute("SELECT * FROM %s" %(round_part))
    col = cur1.fetchone()[0]
    row = cur2.fetchall()

    for count in range(0,col):
        table_name = round_meta+ str(count)
        cur1.execute("SELECT * FROM %s WHERE Rating>=%f and Rating<=%f" % (table_name, ratingMinValue, ratingMaxValue))
        rows = cur1.fetchall()
        for temp1 in rows:
               openfile.write("%s,%s,%s,%s" % (table_name, temp1[0], temp1[1], temp1[2]))
               openfile.write('\n')
    for r in row:
        table_name = range_part+ str(r[0])
        cur1.execute("SELECT * FROM %s WHERE Rating>=%f and Rating<=%f" %(table_name,ratingMinValue,ratingMaxValue))
        rows= cur1.fetchall()
        for temp2 in rows:
            openfile.write("%s,%s,%s,%s" % (table_name, temp2[0], temp2[1], temp2[2]))
            openfile.write('\n')

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur1 = openconnection.cursor()
    cur2 = openconnection.cursor()
    openfile = open('PointQueryOut.txt', 'w')
    if ratingValue == 0:
       cur2.execute("SELECT * FROM %s WHERE minrating<=%f and maxrating>=%f" % (range_meta, ratingValue, ratingValue))
    else:
       cur2.execute("SELECT * FROM %s WHERE minrating<%f and maxrating>=%f" % (range_meta, ratingValue, ratingValue))
    cur1.execute("SELECT * FROM %s" % (round_part))
    column = cur1.fetchone()[0]
    row = cur2.fetchall()
    for temp in range(0, column):
        table_name = round_meta+ str(temp)
        cur1.execute("SELECT * FROM %s WHERE Rating=%f" % (table_name, ratingValue))
        rows = cur1.fetchall()
        for temp1 in rows:
            openfile.write("%s,%s,%s,%s" % (table_name, temp1[0], temp1[1], temp1[2]))
            openfile.write('\n')
    for r in row:
        table_name = range_part+ str(r[0])
        cur2.execute("SELECT * FROM %s WHERE Rating=%f" % (table_name, ratingValue))
        rows = cur2.fetchall()
        for temp2 in rows:
            openfile.write("%s,%s,%s,%s" % (table_name, temp2[0], temp2[1], temp2[2]))
            openfile.write('\n')