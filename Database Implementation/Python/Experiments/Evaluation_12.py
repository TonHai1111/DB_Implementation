#!/usr/bin/python
import sys
sys.path.append("/home/tonhai/Data/Depaul/PLIExtension")

import psycopg2
#from PLIExtension import IBTree
from IBTree import *
from PLITypes.IBEntry import *
from PLITypes.dataTypes import *
from tools.operations import *
from IBPlusTree import *
from tools.timeTools import  *
import re
from decimal import *
from random import uniform, randint
import os

MAX_NUM_ROW_BUCKET = 20
MAX_DISTANCE = 999999.0
MIN_DISTANCE = 0.0

def run_insertion_NYC():
    #1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='hai' password='hai' host='localhost' ")
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
    plusTree = IBPlusTree()
    plusTree.copyStructure(tree)
    print "Finish initializing IB-Tree and IB+-Tree\n"
    tree = None
    tree1 = IBTree()
    plusTree.setIBTree(tree1)
    #3. Measuring the insert performance
    t = timer()
    #3.1 Get data from postgresql database
    for pos in range(0, 147): #147550575
        for j in range(0, 100):
            strGetData = "select * from trips where id >= " + str((pos)*1000000 + j * 10000)
            strGetData += " and id < " + str((pos)*1000000 + (j + 1)*10000) + ";"
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
                print str(pos) + ": " + str(j)
        #print str(pos) + "th -- execution time: " + str(t.getResult()) + "(s)"
    print "Total time: " + str(t.getResult())
    tree1.writeMetaData()
    plusTree.writeMetaData()
    print "Finished!"

def run_loadData_DB(sortedFile, numRows, numTuples):
    count = 0
    # 1. Read data from listBuckets_sorted_2.txt
    print "Reading data and inserting into IB-Tree..."
    fin1 = open(sortedFile, "r")
    # 2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    if (numRows <= 0):
        number = input('Enter a number: ')
    else:
        number = numRows
    for line in fin1:
        tokens = line.split(' ')
        if (tokens[0] == "interval"):
            if (count >= number):
                break
            bucketID = int(tokens[1].replace(':', ''))
            interval[0] = float(tokens[2])
            interval[1] = float(tokens[3])
            tree.insertBucket(interval, bucketID)
            count += 1
    # 2.1. Print IB-Tree
    print "Done!"
    print "Printing IB-Tree..."
    tree.printIBTree()
    fin1.close()
    print "Number of buckets: " + str(bucketID + 1)

    # IB+-Tree
    # 3. IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree()
    plusTree.copyStructure(tree)
    t = timer()
    #4. Read data from the database
    #5. Insert data into the IBPlusTree
    count = 0
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='hai' password='hai' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    start = 0
    end = 0
    size = 250000
    trace = 0
    while (end < numTuples):
        if((numTuples - end) > size):
            end = start + size
        else:
            end = start + (numTuples - end)
        strQuery = "select * from trips where id >=" + str(start) + " and id <" + str(end)
        trace += 1
        if(trace % 10 == 0):
            print trace
        cursor.execute(strQuery)
        data = cursor.fetchall()
        t.start()
        for row in data:
            strValue = "%s\n" % ", ".join(map(str, row))
            key = float(row[12])
            plusTree.insertTuple(key, strValue)
        start = end
        t.end()
    #5. Write the metadata info file
    plusTree.flush()
    print "Total time: " + str(t.getResult())
    print "=================<<<<>>>>=================="
    plusTree.writeMetaData()

    #plusTree.readMetaData()

    #tree.writeMetaData()
    plusTree.ibTree.writeMetaData()

    #tree.readMetaData()

    return

def test3(intervals, n):
    count = 0
    # 1. Loading IB-Tree
    print "Loading IB-Tree..."
    # 2. Insert buckets into IB-Tree
    tree = IBTree()
    tree.readMetaData()
    # 2.1. Print IB-Tree
    print "Done!"
    #print "Printing IB-Tree..."
    #tree.printIBTree()

    # 3. Loading IB+-Tree
    print "Loading IB+-Tree"
    plusTree = IBPlusTree()

    plusTree.readMetaData()

    plusTree.setIBTree(tree)
    print "Done!"

    # 4. Query data for a given interval
    for i in range(0, n):
        listBuckets = ListBuckets()
        listTuples = ListTuples()
        tt = timer()
        tt.start()
        plusTree.search(listTuples, listBuckets, intervals[i])
        tt.end()
        # 5. Print result
        print intervals[i]
        print "Buckets (IB+-Tree): ", len(listBuckets.results)
        print "Time1: ", tt.resultInSecond
    #readDB(listBuckets.results, "ibPlusTreeDB.dat")
    return

if __name__ == '__main__':
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    #run_insertion_NYC()
    run_loadData_DB("listBuckets_sorted_4.txt", 140000, 147550000)
    #Result - 1000 tuples per bucket
    #intervals = [[1, 1.03125], [1, 1.0625], [1, 1.125], [1, 1.25], [1, 1.5], [1, 1.75]]
    #[1, 1.03125]    #Buckets(IB + -Tree):  4178    #Time1:  44.0685801506
    #[1, 1.0625]    #Buckets(IB + -Tree):  5210    #Time1:  55.3898308277
    #[1, 1.125]    #Buckets(IB + -Tree):  9807    #Time1:  103.313462019
    #[1, 1.25]    #Buckets(IB + -Tree):  16521    #Time1:  176.618077993
    #[1, 1.5]    #Buckets(IB + -Tree):  30476    #Time1:  326.972563982
    #[1, 1.75]    #Buckets(IB + -Tree):  40398    #Time1:  433.318394184
    intervals = [[1, 1.03125], [1, 1.0625], [1, 1.125], [1, 1.25], [1, 1.5], [1, 1.75]]
    test3(intervals, 6)

    print "---------------------------\n"
