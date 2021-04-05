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

def run_insertion_NYC(position):
    #1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"

    #2. Initialize IB-Tree and IB+-Tree
    #2.1 Get data from postgresql DB (1 000 000 rows)
    strGetData = "select * from trips where id >= 1 and id <=1000000 order by trip_distance;"
    cursor.execute(strGetData)
    data = cursor.fetchall()
    #strInsert = "INSERT INTO trips_bucket (bucketID, bucketData) VALUES (%s, %s)"
    #2.2 Insert into IB-Tree and copy structure to IB+-Tree
    index = 0
    low = Constants.MAX_DISTANCE
    high = Constants.MIN_DISTANCE
    interval = [0.0 for x in range(2)]
    bucketID = 10
    tree = IBTree()
    temp = ""
    for row in data:
        index += 1
        distance = Decimal(row[12])
        if(low > distance):
            low = distance
        if(high < distance):
            high = distance
        for j in range(0,len(row)):
            temp += str(row[j])
        if(index == 1000):
            bucketID += 1
            interval[0] = float(low)
            interval[1] = float(high)
            # Insert into IB-Tree
            tree.insertBucket(interval, bucketID)
            # Write data into postgresql (not necessary)
            #cursor.execute(strInsert, (bucketID, temp))
            #conn.commit()
            index = 0
            low = Constants.MAX_DISTANCE
            high = Constants.MIN_DISTANCE
    #print "Printing IB-Tree..."
    #tree.printIBTree()
    #IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree(tree)
    plusTree.copyStructure(tree)
    print "Finish initializing IB-Tree and IB+-Tree. Start to evaluate (20 times)...\n"
    tree = None
    tree1 = IBTree()
    plusTree.ibTree = tree1
    #3. Repeat 20 times, measuring the insert performance
    t = timer()
    #3.1 Get data from postgresql database
    for j in range(0, 100):
        strGetData = "select * from trips where id >= " + str((position)*1000000 + j * 10000)
        strGetData += " and id < " + str((position)*1000000 + (j + 1)*10000) + ";"
        #print strGetData
        cursor.execute(strGetData)
        data = cursor.fetchall()
        #3.2 Insert into IB+-Tree and measure the time
        t.start()
        for row in data:
            key = Decimal(row[12])
            temp = ""
            for k in range(0, len(row)):
                temp += str(row[k])
            plusTree.insertTuple(key, temp)
        t.end()
        if(j % 10 == 0):
            print str(j)
    print str(position) + "th -- execution time: " + str(t.getResult()) + "(s)"
    print "Finished!"

def run_insertion_BD(position, start, end):
    #1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='BigDataTest' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the BigDataTest database!"
        return
    print "Connected!"

    #2. Initialize IB-Tree and IB+-Tree
    #2.1 Get data from postgresql DB (1 000 000 rows)


    strGetData = "select t1.pt from (select * from muon where ctid >= \'(0, 1)\' and ctid <=\'(30303, 1)\') as t1 where "
    strGetData += "t1.pt >= 3 and t1.pt < 403;"

    cursor.execute(strGetData)
    data = cursor.fetchall()
    #strInsert = "INSERT INTO trips_bucket (bucketID, bucketData) VALUES (%s, %s)"
    #2.2 Insert into IB-Tree and copy structure to IB+-Tree
    index = 0
    low = Constants.MAX_DISTANCE
    high = Constants.MIN_DISTANCE
    interval = [0.0 for x in range(2)]
    bucketID = 10
    tree = IBTree()
    temp = ""
    for row in data:
        index += 1
        distance = Decimal(row[0])
        if(low > distance):
            low = distance
        if(high < distance):
            high = distance
        for j in range(0,len(row)):
            temp += str(row[j])
        if(index == 1000):
            bucketID += 1
            interval[0] = float(low)
            interval[1] = float(high)
            # Insert into IB-Tree
            tree.insertBucket(interval, bucketID)
            # Write data into postgresql (not necessary)
            #cursor.execute(strInsert, (bucketID, temp))
            #conn.commit()
            index = 0
            low = Constants.MAX_DISTANCE
            high = Constants.MIN_DISTANCE
    #print "Printing IB-Tree..."
    #tree.printIBTree()
    #IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree(tree)
    plusTree.copyStructure(tree)
    print "Finish initializing IB-Tree and IB+-Tree. Start to evaluate (20 times)...\n"
    tree = None
    tree1 = IBTree()
    plusTree.ibTree = tree1
    #3. Repeat 20 times, measuring the insert performance
    t = timer()
    #3.1 Get data from postgresql database
    #for j in range(0, 100):
    strGetData = "select pt from muon where ctid >= \'" + str(start)
    strGetData += "\' and ctid <= \'" + str(end) + "\';"
    #print strGetData
    cursor.execute(strGetData)
    data = cursor.fetchall()
    #3.2 Insert into IB+-Tree and measure the time
    t.start()
    for row in data:
        key = Decimal(row[0])
        temp = ""
        for k in range(0, len(row)):
            temp += str(row[k])
        plusTree.insertTuple(key, temp)
    t.end()
        #if(j % 10 == 0):
        #    print str(j)
    print str(position) + "th -- execution time: " + str(t.getResult()) + "(s)"
    print "Finished!"

def evaluation_BTree(interval):
    #1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"

    #2. Run query
    tt = timer()
    tt.start()
    strQuery = "select count(*) from trips_v where id >= " + str(interval[0]) + " and id <=" + str(interval[1]) + ";"
    cursor.execute(strQuery)
    data = cursor.fetchall()
    tt.end()
    for row in data:
        value = Decimal(row[0])
        print (value)
    print "Time: ", tt.resultInSecond
    print "Finished!"

def evaluation_Clustered_Unclustered(interval):
    #1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"

    #2. Run query
    tt = timer()
    tt.start()
    if(interval[0] >= 0.0):
        strQuery = "select count(*) from trips_v where trip_distance > " + str(interval[0]) + " and trip_distance <=" + str(interval[1]) + ";"
    else:
        strQuery = "select count(*) from trips_v;"
    cursor.execute(strQuery)
    data = cursor.fetchall()
    tt.end()
    for row in data:
        value = Decimal(row[0])
        print (value)
    if(interval[0] >= 0.0):
        print "Interval: [" + str(interval[0]) + ", " + str(interval[1]) + "]"
    else:
        print "Table scan: [Min, Max]"
    print "Time: ", tt.resultInSecond
    print "Finished!"

if __name__ == '__main__':
    #for i in range(0, 20):
    #    run_insertion_NYC(i)
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 3 > /proc/sys/vm/drop_caches\'")
    evaluation_Clustered_Unclustered([1.5, 1.573])
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 3 > /proc/sys/vm/drop_caches\'")
    evaluation_Clustered_Unclustered([1.5, 1.75])
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 3 > /proc/sys/vm/drop_caches\'")
    evaluation_Clustered_Unclustered([1.5, 2])
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 3 > /proc/sys/vm/drop_caches\'")
    evaluation_Clustered_Unclustered([1.5, 3])
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 3 > /proc/sys/vm/drop_caches\'")
    evaluation_Clustered_Unclustered([1, 3])
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 3 > /proc/sys/vm/drop_caches\'")
    evaluation_Clustered_Unclustered([1, 3.5])
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 3 > /proc/sys/vm/drop_caches\'")
    evaluation_Clustered_Unclustered([-1, -2])
    #run_insertion_BD(0, "(0,1)", "(30303, 1)")
    #run_insertion_BD(1, "(30303,2)", "(60606, 1)")
    #run_insertion_BD(2, "(60606,2)", "(90909, 1)")
    #run_insertion_BD(3, "(90909,2)", "(121212, 1)")
    #run_insertion_BD(4, "(121212,2)", "(151515, 1)")
    #run_insertion_BD(5, "(151515,2)", "(181818, 1)")
    #run_insertion_BD(6, "(15151,1)", "(45454, 30)")
    #run_insertion_BD(7, "(45454,1)", "(75757, 30)")
    #run_insertion_BD(8, "(75757,1)", "(106060, 30)")
    #run_insertion_BD(9, "(106060,1)", "(136363, 30)")

    #run_insertion_BD(10, "(136363,1)", "(166666, 30)")
    #run_insertion_BD(11, "(161666,1)", "(191931, 27)")
    #run_insertion_BD(12, "(10101,1)", "(40404, 1)")
    #run_insertion_BD(13, "(40404,1)", "(70707, 1)")
    #run_insertion_BD(14, "(70707,1)", "(101010, 1)")
    #run_insertion_BD(15, "(101010,1)", "(131313, 1)")
    #run_insertion_BD(16, "(131313,1)", "(161616, 1)")
    #run_insertion_BD(17, "(161616,1)", "(191919, 1)")
    #run_insertion_BD(18, "(20202,1)", "(50505, 1)")
    #run_insertion_BD(19, "(50505,1)", "(80808, 1)")
    #calculate_entropy_improved_BD([3, 403], "(0,1)", "(30303, 1)") # Entropy: 0.772180686631
    #calculate_entropy_improved_BD([3, 403], "(30303,2)", "(60606, 1)") # Entropy: 0.814618599738
    #calculate_entropy_improved_BD([3, 403], "(60606,2)", "(90909, 1)") # Entropy: 0.853235796929
    #calculate_entropy_improved_BD([3, 403], "(90909,2)", "(121212, 1)") # Entropy: 0.761553483309
    #calculate_entropy_improved_BD([3, 403], "(121212,2)", "(151515, 1)") # Entropy: 0.739006066987
    #calculate_entropy_improved_BD([3, 403], "(151515,2)", "(181818, 1)") # Entropy: 0.738978990847

    #calculate_entropy_improved_BD([3, 403], "(15151,1)", "(45454, 30)") # Entropy: 0.78731590929
    #calculate_entropy_improved_BD([3, 403], "(45454,1)", "(75757, 30)") # Entropy: 0.836071830978   Repeat
    #calculate_entropy_improved_BD([3, 403], "(75757,1)", "(106060, 30)") # Entropy: 0.825303829425
    #calculate_entropy_improved_BD([3, 403], "(106060,1)", "(136363, 30)") # Entropy: 0.739109335735
    #calculate_entropy_improved_BD([3, 403], "(136363,1)", "(166666, 30)") # Entropy: 0.738728858556
    #calculate_entropy_improved_BD([3, 403], "(161666,1)", "(191931, 27)") # Entropy: 0.73874200733

    #calculate_entropy_improved_BD([3, 403], "(10101,1)", "(40404, 1)") # Entropy: 0.77965513063
    #calculate_entropy_improved_BD([3, 403], "(40404,1)", "(70707, 1)") # Entropy: 0.829813186314
    #calculate_entropy_improved_BD([3, 403], "(70707,1)", "(101010, 1)") # Entropy: 0.842103147726
    #calculate_entropy_improved_BD([3, 403], "(101010,1)", "(131313, 1)") # Entropy: 0.739070160059
    #calculate_entropy_improved_BD([3, 403], "(131313,1)", "(161616, 1)") # Entropy: 0.738893146587
    #calculate_entropy_improved_BD([3, 403], "(161616,1)", "(191919, 1)") # Entropy: 0.73876064284

    #calculate_entropy_improved_BD([3, 403], "(20202,1)", "(50505, 1)") # Entropy: 0.794065494633
    #calculate_entropy_improved_BD([3, 403], "(50505,1)", "(80808, 1)") # Entropy: 0.844237894757
    print "-----------------------\n"