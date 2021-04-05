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
from PLI import PLI
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

def run_loadData(sortedFile, numRows1, randomFile, numRows2, databaseFile="", metadataFile=""):
    aPLI = PLI()
    if((databaseFile != "") & (metadataFile != "")):
        aPLI.setFile(databaseFile, metadataFile)
    count = 0
    #1. Read data from the sorted file
    #2. Insert data into the clustered part
    fin1 = open(sortedFile, "r")
    for line in fin1:
        if(count >= numRows1):
            break
        if(re.match('^#.*$', line)):
            continue
        tokens = line.split(' ')
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue
        count += 1
        values = line.rstrip('\n').split(', ')
        key = float(values[12])
        data = line.rstrip('\n')
        aPLI.insertTuple(key, data)
    fin1.close()
    #3. Read data from the random file
    #4. Insert data into the overflow file
    count = 0
    fin2 = open(randomFile, "r")
    for line in fin2:
        if(count >= numRows2):
            break
        if(re.match('^#.*$', line)):
            continue
        tokens = line.split(' ')
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue
        count += 1
        values = line.rstrip('\n').split(', ')
        key = float(values[12])
        data = line.rstrip('\n')
        aPLI.insertTupleOverflow(key, data)
    fin2.close()
    #5. Write the metadata info file
    aPLI.writeMetaData()
    return

def run_loadData_DB(sortedFile, numRows, numTuples, databaseFile="", metadataFile=""):
    aPLI = PLI()
    if((databaseFile != "") & (metadataFile != "")):
        aPLI.setFile(databaseFile, metadataFile)
    count = 0
    #1. Read data from the sorted file
    #2. Insert data into the clustered part
    fin1 = open(sortedFile, "r")
    for line in fin1:
        if(count >= numRows):
            break
        if(re.match('^#.*$', line)):
            continue
        tokens = line.split(' ')
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue
        count += 1
        values = line.rstrip('\n').split(', ')
        key = float(values[12])
        data = line.rstrip('\n')
        aPLI.insertTuple(key, data)
    fin1.close()
    #3. Read data from the database
    #4. Insert data into the overflow page
    count = 0
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    start = 0
    end = 0
    size = 500000
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
        for row in data:
            strValue = "%s\n" % ", ".join(map(str, row))
            key = float(row[12])
            aPLI.insertTupleOverflow(key, strValue)
        start = end
    #5. Write the metadata info file
    aPLI.writeMetaData()
    return

def run_test(_output, Query, databaseFile="", metadataFile=""):
    aPLI = PLI()
    if((databaseFile != "") & (metadataFile != "")):
        aPLI.setFile(databaseFile, metadataFile)
    #1. Read the metadata from file
    aPLI.readMetaData()
    #2. Run the query: Q1, Q2, Q3, Q4, Q5
    aPLI.searchTuples(_output, Query)
    return

if __name__ == '__main__':
    #run_loadData_DB("listBuckets_sorted_4.txt", 3000000, 147550000, "PLIdataDB.dat", "PLImetaDB.dat")
    output1 = []
    output2 = []
    output3 = []
    output4 = []
    output5 = []
    Q1 = [1, 1.03125]
    Q2 = [1, 1.0625]
    Q3 = [1, 1.125]
    Q4 = [1, 1.25]
    Q5 = [1, 1.5]

    tt = timer()
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    tt.start()
    run_test(output1, Q1, "PLIdataDB.dat", "PLImetaDB.dat")
    tt.end()
    print tt.getResultInSecond()
    print len(output1)
    print "---------------------------\n"


    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    tt.reStart()
    run_test(output2, Q2, "PLIdataDB.dat", "PLImetaDB.dat")
    tt.end()
    print tt.getResultInSecond()
    print len(output2)
    print "---------------------------\n"

    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    tt.reStart()
    run_test(output3, Q3, "PLIdataDB.dat", "PLImetaDB.dat")
    tt.end()
    print tt.getResultInSecond()
    print len(output3)
    print "---------------------------\n"

    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    tt.reStart()
    run_test(output4, Q4, "PLIdataDB.dat", "PLImetaDB.dat")
    tt.end()
    print tt.getResultInSecond()
    print len(output4)
    print "---------------------------\n"

    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    tt.reStart()
    run_test(output5, Q5, "PLIdataDB.dat", "PLImetaDB.dat")
    tt.end()
    print tt.getResultInSecond()
    print len(output5)
    print "---------------------------\n"
    print "---------------------------\n"