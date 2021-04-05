#!/usr/bin/python

#TODO:
# 1. Read data from BigData_listBuckets_sorted.txt
# 2. Insert bucketData into BigDataTest database (MuonBucket table)
# 3. Record the BucketID and CTID into file

# 4. Do the test: Retrieve the bucket data using CTID
# 5. Measure the execution time
# 6. Write the output to file

import psycopg2
from IBTree import IBTree
from PLITypes.IBEntry import *
from PLITypes.dataTypes import *
from tools.operations import *
from IBPlusTree import IBPlusTree
from tools.timeTools import  *
import re
from decimal import *
from random import uniform, randint
import os
import math

MAX_NUM_ROW_BUCKET = 20
MAX_DISTANCE = 999999.0
MIN_DISTANCE = 0.0

def run_clustering_NYC():
    #1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"

    t = timer()
    strClustering1 = "create index trips_dis on trips_i(trip_distance);cluster trips_i using trips_dis;"
    strClustering2 = "create index trips_dis on trips_ii(trip_distance);cluster trips_ii using trips_dis;"
    strClustering3 = "create index trips_dis3 on trips_iii(trip_distance);cluster trips_iii using trips_dis3;"
    strClustering4 = "create index trips_dis4 on trips_iv(trip_distance);cluster trips_iv using trips_dis4;"
    strClustering5 = "create index trips_dis5 on trips_v(trip_distance);cluster trips_v using trips_dis5;"
    #print "Clustering trips_i..."
    #t.start()
    #cursor.execute(strClustering1)
    ##cursor.execute(strClustering11)
    #t.end()
    #print "i -- clustering time: " + str(t.getResult()) + "(s)"

    #print "Clustering trips_ii..."
    #t2 = timer()
    #t2.start()
    #cursor.execute(strClustering2)
    ##cursor.execute(strClustering22)
    #t2.end()
    #print "ii -- clustering time: " + str(t2.getResult()) + "(s)"

    #print "Clustering trips_iii..."
    #t3 = timer()
    #t3.start()
    #cursor.execute(strClustering3)
    ##cursor.execute(strClustering33)
    #t3.end()
    #print "iii -- clustering time: " + str(t3.getResult()) + "(s)"

    #print "Clustering trips_iv..."
    #t4 = timer()
    #t4.start()
    #cursor.execute(strClustering4)
    ##cursor.execute(strClustering44)
    #t4.end()
    #print "iv -- clustering time: " + str(t4.getResult()) + "(s)"

    print "Clustering trips_v..."
    t5 = timer()
    t5.start()
    cursor.execute(strClustering5)
    # cursor.execute(strClustering44)
    t5.end()
    print "iv -- clustering time: " + str(t5.getResult()) + "(s)"

    print "Finished!"

if __name__ == '__main__':
    run_clustering_NYC()
    print "-----------------------\n"