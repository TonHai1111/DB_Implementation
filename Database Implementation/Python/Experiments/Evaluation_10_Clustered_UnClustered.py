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

MAX_NUM_ROW_BUCKET = 20
MAX_DISTANCE = 999999.0
MIN_DISTANCE = 0.0

def run_testing2(start, end):
    #print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    #print "Connected!"

    #analyzeQuery = "explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.5 "
    normalQuery = "select * from trips_i where trip_distance >= " + str(start) + " and trip_distance < " + str(end)
    #normalQuery = "select * from trips_cluster where trip_distance = 1.0"
    #cursor.execute(analyzeQuery)
    windows = 0.025
    tt = timer()
    tt.start()
    cursor.execute(normalQuery)
    tt.end()
    #data = cursor.fetchall()
    #counter = 0
    #while True:
    #    counter += 1
    #    normalQuery =
    #    print counter
    #    tt.reStart(tt.result)
    #    data = cursor.fetchmany(windows)
    #    tt.end()
    print tt.getResultInSecond()
    #print data
    #print "Finished!"

#Query:
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.03125
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.0625
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.125
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.25
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.5
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 2
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 3.5

if __name__ == '__main__':
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    print "----------Query 0---------"
    run_testing2(1.0, 1.03125)
    print "---------------------------\n"
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    print "----------Query 1---------"
    run_testing2(1.0, 1.0625)
    print "---------------------------\n"
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    print "----------Query 2---------"
    #run_testing2(1.0, 1.0625)
    #run_testing2(1.0625, 1.125)
    run_testing2(1.0, 1.125)
    print "---------------------------\n"
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    print "----------Query 3---------"
    #run_testing2(1.0, 1.0625)
    #run_testing2(1.0625, 1.125)
    #run_testing2(1.125, 1.1875)
    #run_testing2(1.1875, 1.25)
    run_testing2(1.0, 1.25)
    print "---------------------------\n"
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    print "----------Query 4---------"
    #run_testing2(1.0, 1.0625)
    #run_testing2(1.0625, 1.125)
    #run_testing2(1.125, 1.1875)
    #run_testing2(1.1875, 1.25)
    #run_testing2(1.25, 1.2625)
    #run_testing2(1.2625, 1.325)
    #run_testing2(1.325, 1.3875)
    #run_testing2(1.3875, 1.5)
    run_testing2(1.0, 1.5)
    print "---------------------------\n"