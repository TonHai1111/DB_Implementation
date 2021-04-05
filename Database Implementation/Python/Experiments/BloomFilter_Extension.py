#!/usr/bin/python

#Descriptions:
# 1. Read data from PostgresQL (Tables: trips_proj5
#   (clustered on trip_distance), trips_proj6 (clustered on tip_amount),
#   trips_proj7 (clustered on total_amount))
# 2. Build the bloomFilter on id columns of all above tables (10000 col/bucket)
# 3. Write the output to trips_proj5_bf.dat, trips_proj6_bf.dat and
#   trips_proj7_bf.dat

# 4. Do the test: Read these output files and do the joining between columns
# 5. Report the amount of data accessed.
# 6. ...

import psycopg2
from IBTree import IBTree
from PLITypes.IBEntry import *
from PLITypes.dataTypes import *
from tools.operations import *
from IBPlusTree import IBPlusTree
from tools.timeTools import  *
from tools.generalTools import  *
import re
from decimal import *
from random import uniform, randint
import os
import math
from os import listdir
from os.path import isfile, join

MAX_NUM_LOAD = 2400000
MAX_ROW_DB = 295101150
MAX_NUM_ROW_BUCKET_COLUMNAR = 12000
#NUM_MILESTONE = 8
#NUM_INTERVAL = NUM_MILESTONE / 2 - 1
MAX_BF_PAGE = 100
UNCOMPRESSED = 0
COMPRESSED = 1
MAX_VALUE = 400000000 # used for BF hashing function

def buildBF(listofID, count, output):
    # 1. We selected 512bytes for each BF representing a bucket of 12000 rows.
    #   There will be 120 pages, each page represents 100 rows.
    # 2. The hashing method is the hex respresentation of a number.
    #   This means it contains 32 hash functions. They are the hex layout
    #   of a number.
    # 3. Selected hash function:
    #   if 0 <= X <= 400 000 000 then X = X * (X%10 + 1)
    #   else X = X
    # where X is an unsigned integer (4 bytes: 0 to 4 294 967 295)
    if(count > 0 and listofID != None):
        curBF = 0
        index = 0
        curNumRows = 0
        curOutputIndex = 0
        curID = 0
        output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE] = MAX_VALUE #outMinK = MAX_VALUE
        output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1] = 0 #outMaxK = 0
        while (index < count):
            curID = listofID[index]
            # Min, Max
            if (curID > output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1]): #outMaxK
                output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1] = curID
            if (curID < output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE]): #outMinK
                output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE] = curID
            # build BF
            if(curID < MAX_VALUE):
                curID = curID * (curID % 10 + 1)
            curBF = curBF or curID
            curNumRows += 1
            index += 1
            if(curNumRows >= MAX_BF_PAGE):
                # write to output
                output[curOutputIndex] = curBF
                curOutputIndex += 1
                # reset values
                curBF = 0
                curNumRows = 0
        #last page
        if(curNumRows > 0):
            # write to output
            output[curOutputIndex] = curBF
            curOutputIndex += 1
            # reset values
            curBF = 0
            curNumRows = 0
    return

def checkBF(BF, list_ID):
    if(BF != None and list_ID != None):
        length =  len(list_ID)
    for i in range(length):
        curID = list_ID[i]
        if(curID == 0):
            continue
        for j in range(MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE):
            # Checking
            if((curID < BF[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE]) or (curID > BF[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1])):
                continue
            if(curID < MAX_VALUE):
                curID = curID * (curID % 10 + 1)
            if (not((curID & BF[j]) ^ curID)):
                return True
    return False

def buildBF_2(listofID, count, output):
    # 1. We selected 512bytes for each BF representing a bucket of 12000 rows.
    #   There will be 120 pages, each page represents 100 rows.
    # 2. The hashing method is the hex respresentation of a number.
    #   This means it contains 32 hash functions. They are the hex layout
    #   of a number.
    # 3. Selected hash function:
    #   X = 1 << (X%10)
    # where X is an unsigned integer (4 bytes: 0 to 4 294 967 295)
    if(count > 0 and listofID != None):
        curBF = 0
        index = 0
        curNumRows = 0
        curOutputIndex = 0
        curID = 0
        output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE] = MAX_VALUE #outMinK = MAX_VALUE
        output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1] = 0 #outMaxK = 0
        while (index < count):
            curID = listofID[index]
            # Min, Max
            if (curID > output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1]): #outMaxK
                output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1] = curID
            if (curID < output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE]): #outMinK
                output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE] = curID
            # build BF
            if(curID < MAX_VALUE):
                curID = 1 << (curID % 10)
            curBF = curBF or curID
            curNumRows += 1
            index += 1
            if(curNumRows >= MAX_BF_PAGE):
                # write to output
                output[curOutputIndex] = curBF
                curOutputIndex += 1
                # reset values
                curBF = 0
                curNumRows = 0
        #last page
        if(curNumRows > 0):
            # write to output
            output[curOutputIndex] = curBF
            curOutputIndex += 1
            # reset values
            curBF = 0
            curNumRows = 0
    return

def checkBF_2(BF, list_ID):
    if(BF != None and list_ID != None):
        length = len(list_ID)
    for i in range(length):
        curID = list_ID[i]
        if(curID == 0):
            continue
        for j in range(MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE):
            # Checking
            if((curID < BF[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE]) or (curID > BF[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1])):
                continue
            if(curID < MAX_VALUE):
                curID = 1 << (curID % 10)
            if (not((curID & BF[j]) ^ curID)):
                return True
    return False

def buildBF_3(listofID, count, output):
    # 1. We selected 512bytes for each BF representing a bucket of 12000 rows.
    #   There will be 120 pages, each page represents 100 rows.
    # 2. The hashing method is the hex respresentation of a number.
    #   This means it contains 32 hash functions. They are the hex layout
    #   of a number.
    # 3. Selected hash function:
    #   X = 1 << (X%3840)
    # where X is an unsigned integer (4 bytes: 0 to 4 294 967 295)
    if(count > 0 and listofID != None):
        index = 0
        curID = 0
        output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE] = MAX_VALUE #outMinK = MAX_VALUE
        output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1] = 0 #outMaxK = 0
        while (index < count):
            curID = listofID[index]
            # Min, Max
            if (curID > output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1]): #outMaxK
                output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1] = curID
            if (curID < output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE]): #outMinK
                output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE] = curID
            # build BF
            value = curID % 3840
            BFPos = value / 32
            bitPos = value % 32
            output[BFPos] = 1 << bitPos
            index += 1
    return

def checkBF_3(BF, list_ID):
    if(BF != None and list_ID != None):
        length = len(list_ID)
    IDBF = [0 for x in range(MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 2)]

    for i in range(length):
        curID = list_ID[i]
        if(curID == 0):
            continue
        if((curID < BF[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE]) or (curID > BF[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1])):
            continue
        #build IDBF
        value = curID % 3840
        BFPos = value / 32
        bitPos = value % 32
        IDBF[BFPos] = 1 << bitPos
    #Check BF
    for i in range(MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE):
        if((BF[i] & IDBF[i]) != 0):
            return True
    return False

def buildBloomFilter_FromDB_3(outputFB, mode):
    # 1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111'")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"
    # 2. Get data from database to build imprints
    # Output files
    foutBF = open(outputFB, 'a')

    if(mode == 0):# trip_distance
        array = [0.5, 0.7, 0.85, 1.0, 1.15, 1.3, 1.45, 1.6, 1.75, 2.0, 2.3, 2.6, 2.8, 3.0, 3.5, 4.0, 5.0, 7.0, 10.0]
        tableName = 'trips5'
        colName = 'trip_distance'
    elif(mode == 1): # tip_amount
        array = [0.0, 0.3, 0.6, 0.8, 1.0, 1.2, 1.6, 1.8, 2.0, 2.35, 2.7, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        tableName = 'trips6'
        colName = 'tip_amount'
    else: # total_amount
        array = [6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 15.0, 17.0, 19.0, 22.0, 25.0, 40.0]
        tableName = 'trips7'
        colName = 'total_amount'
    length = len(array)
    print "Total iterations: " + str(length + 1)
    numBucket = 0;
    count = 0;
    index = 0;
    listID = [0 for x in range(MAX_NUM_ROW_BUCKET_COLUMNAR)]
    output = [0 for x in range(MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 2)]
    #outMinK = output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE]
    #outMaxK = output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1]
    loop = 0 #Only use for the mode 1
    i = 0
    while (i < length + 1):
        print "Round i: " + str(i)
        if(mode == 1): # Mode 1
            if(i == 0): # Loop for 6 times
                query = "select id from " + str(tableName) + " where " + str(colName) + " <= " + str(array[i]) + " offset " + str(loop * 20000000) + " rows fetch first 20000000 rows only;"
                print "\tLoop: " + str(loop)
                loop = loop + 1
                if(loop < 6):
                    i = i - 1 #Continue to loop until 6
            elif(i == length):
                query = "select id from " + str(tableName) + " where " + str(colName) + " > " + str(array[i - 1])+ ";"
            else:
                query = "select id from " + str(tableName) + " where " + str(colName) + " > " + str(array[i - 1]) + " and " + str(colName) + " <= " + str(array[i]) + ";"
        else: # Mode 0 & 2
            if(i == 0):
                query = "select id from " + str(tableName) + " where " + str(colName) + " < " + str(array[i]) + ";"
            elif(i == length):
                query = "select id from " + str(tableName) + " where " + str(colName) + " >= " + str(array[i - 1])+ ";"
            else:
                query = "select id from " + str(tableName) + " where " + str(colName) + " >= " + str(array[i - 1]) + " and " + str(colName) + " < " + str(array[i]) + ";"
        print "\tQuery: " + str(query)
        cursor.execute(query)
        data = cursor.fetchall()
        total_rows = len(data)
        print "\tTotal rows: " + str(total_rows)
        for row in data:
            #insert to a list
            listID[count] = int(row[0])
            count = count + 1
            index = index + 1
            if(count >= MAX_NUM_ROW_BUCKET_COLUMNAR):
                #build BloomFilter pattern
                buildBF_3(listID, count, output)
                #3. Output the BloomFirter pattern to files
                writeBF(output, foutBF, UNCOMPRESSED)
                count = 0
                numBucket = numBucket + 1
        i = i + 1

    if(count > 0): #last row
        #build BloomFilter pattern
        buildBF_3(listID, count, output)
        #3. Output the BloomFirter pattern to files
        writeBF(output, foutBF, UNCOMPRESSED)
        print "\t\tNumber tuples of last bucket: " + str(count)
        count = 0
        numBucket = numBucket + 1

    #print "\tTotal buckets: " + str(numBucket)
    print "\tTotal tuples: " + str(index)
    foutBF.close()
    print "Total number of buckets: " + str(numBucket) + "!\n"
    print "Finished!"
    return

def writeBF(BF, outputFile, mode):
    if (outputFile != None):
        if(mode == UNCOMPRESSED): #uncompressed mode
            for i in range(MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 2):
                outputFile.write(str(BF[i]) + "\n")
            min_pid = 0 # TODO: fill the exact value of the BF, to improve the reading speed
            max_pid = min_pid + 0
            reserve = 0
            outputFile.write(str(min_pid) + "\n")
            outputFile.write(str(max_pid) + "\n")
            outputFile.write(str(reserve) + "\n") # reserve for the future use
            outputFile.write(str(reserve) + "\n")
            outputFile.write(str(reserve) + "\n")
            outputFile.write(str(reserve) + "\n")
        else: #compressed mode
            for i in range(NUM_MILESTONE):
                #TODO: Write in the compressed mode
                outputFile.write(BF[i])
    else:
        print("Error: writeBF function: Null output files!")
    return
def buildBloomFilter_FromDB_2(outputFB, mode):
    # 1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111'")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"
    # 2. Get data from database to build imprints
    # Output files
    foutBF = open(outputFB, 'a')

    if(mode == 0):# trip_distance
        array = [0.5, 0.7, 0.85, 1.0, 1.15, 1.3, 1.45, 1.6, 1.75, 2.0, 2.3, 2.6, 2.8, 3.0, 3.5, 4.0, 5.0, 7.0, 10.0]
        tableName = 'trips5'
        colName = 'trip_distance'
    elif(mode == 1): # tip_amount
        array = [0.0, 0.3, 0.6, 0.8, 1.0, 1.2, 1.6, 1.8, 2.0, 2.35, 2.7, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        tableName = 'trips6'
        colName = 'tip_amount'
    else: # total_amount
        array = [6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 15.0, 17.0, 19.0, 22.0, 25.0, 40.0]
        tableName = 'trips7'
        colName = 'total_amount'
    length = len(array)
    print "Total iterations: " + str(length + 1)
    numBucket = 0;
    count = 0;
    index = 0;
    listID = [0 for x in range(MAX_NUM_ROW_BUCKET_COLUMNAR)]
    output = [0 for x in range(MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 2)]
    #outMinK = output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE]
    #outMaxK = output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1]
    loop = 0 #Only use for the mode 1
    i = 0
    while (i < length + 1):
        print "Round i: " + str(i)
        if(mode == 1): # Mode 1
            if(i == 0): # Loop for 6 times
                query = "select id from " + str(tableName) + " where " + str(colName) + " <= " + str(array[i]) + " offset " + str(loop * 20000000) + " rows fetch first 20000000 rows only;"
                print "\tLoop: " + str(loop)
                loop = loop + 1
                if(loop < 6):
                    i = i - 1 #Continue to loop until 6
            elif(i == length):
                query = "select id from " + str(tableName) + " where " + str(colName) + " > " + str(array[i - 1])+ ";"
            else:
                query = "select id from " + str(tableName) + " where " + str(colName) + " > " + str(array[i - 1]) + " and " + str(colName) + " <= " + str(array[i]) + ";"
        else: # Mode 0 & 2
            if(i == 0):
                query = "select id from " + str(tableName) + " where " + str(colName) + " < " + str(array[i]) + ";"
            elif(i == length):
                query = "select id from " + str(tableName) + " where " + str(colName) + " >= " + str(array[i - 1])+ ";"
            else:
                query = "select id from " + str(tableName) + " where " + str(colName) + " >= " + str(array[i - 1]) + " and " + str(colName) + " < " + str(array[i]) + ";"
        print "\tQuery: " + str(query)
        cursor.execute(query)
        data = cursor.fetchall()
        total_rows = len(data)
        print "\tTotal rows: " + str(total_rows)
        for row in data:
            #insert to a list
            listID[count] = int(row[0])
            count = count + 1
            index = index + 1
            if(count >= MAX_NUM_ROW_BUCKET_COLUMNAR):
                #build BloomFilter pattern
                buildBF_2(listID, count, output)
                #3. Output the BloomFirter pattern to files
                writeBF(output, foutBF, UNCOMPRESSED)
                count = 0
                numBucket = numBucket + 1
        i = i + 1

    if(count > 0): #last row
        #build BloomFilter pattern
        buildBF_2(listID, count, output)
        #3. Output the BloomFirter pattern to files
        writeBF(output, foutBF, UNCOMPRESSED)
        print "\t\tNumber tuples of last bucket: " + str(count)
        count = 0
        numBucket = numBucket + 1

    #print "\tTotal buckets: " + str(numBucket)
    print "\tTotal tuples: " + str(index)
    foutBF.close()
    print "Total number of buckets: " + str(numBucket) + "!\n"
    print "Finished!"
    return

def buildBloomFilter_FromDB(outputFB, mode):
    # 1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111'")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"
    # 2. Get data from database to build imprints
    # Output files
    foutBF = open(outputFB, 'a')

    if(mode == 0):# trip_distance
        array = [0.5, 0.7, 0.85, 1.0, 1.15, 1.3, 1.45, 1.6, 1.75, 2.0, 2.3, 2.6, 2.8, 3.0, 3.5, 4.0, 5.0, 7.0, 10.0]
        tableName = 'trips5'
        colName = 'trip_distance'
    elif(mode == 1): # tip_amount
        array = [0.0, 0.3, 0.6, 0.8, 1.0, 1.2, 1.6, 1.8, 2.0, 2.35, 2.7, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        tableName = 'trips6'
        colName = 'tip_amount'
    else: # total_amount
        array = [6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 15.0, 17.0, 19.0, 22.0, 25.0, 40.0]
        tableName = 'trips7'
        colName = 'total_amount'
    length = len(array)
    print "Total iterations: " + str(length + 1)
    numBucket = 0;
    count = 0;
    index = 0;
    listID = [0 for x in range(MAX_NUM_ROW_BUCKET_COLUMNAR)]
    output = [0 for x in range(MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 2)]
    #outMinK = output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE]
    #outMaxK = output[MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 1]
    loop = 0 #Only use for the mode 1
    i = 0
    while (i < length + 1):
        print "Round i: " + str(i)
        if(mode == 1): # Mode 1
            if(i == 0): # Loop for 6 times
                query = "select id from " + str(tableName) + " where " + str(colName) + " <= " + str(array[i]) + " offset " + str(loop * 20000000) + " rows fetch first 20000000 rows only;"
                print "\tLoop: " + str(loop)
                loop = loop + 1
                if(loop < 6):
                    i = i - 1 #Continue to loop until 6
            elif(i == length):
                query = "select id from " + str(tableName) + " where " + str(colName) + " > " + str(array[i - 1])+ ";"
            else:
                query = "select id from " + str(tableName) + " where " + str(colName) + " > " + str(array[i - 1]) + " and " + str(colName) + " <= " + str(array[i]) + ";"
        else: # Mode 0 & 2
            if(i == 0):
                query = "select id from " + str(tableName) + " where " + str(colName) + " < " + str(array[i]) + ";"
            elif(i == length):
                query = "select id from " + str(tableName) + " where " + str(colName) + " >= " + str(array[i - 1])+ ";"
            else:
                query = "select id from " + str(tableName) + " where " + str(colName) + " >= " + str(array[i - 1]) + " and " + str(colName) + " < " + str(array[i]) + ";"
        print "\tQuery: " + str(query)
        cursor.execute(query)
        data = cursor.fetchall()
        total_rows = len(data)
        print "\tTotal rows: " + str(total_rows)
        for row in data:
            #insert to a list
            listID[count] = int(row[0])
            count = count + 1
            index = index + 1
            if(count >= MAX_NUM_ROW_BUCKET_COLUMNAR):
                #build BloomFilter pattern
                buildBF(listID, count, output)
                #3. Output the BloomFirter pattern to files
                writeBF(output, foutBF, UNCOMPRESSED)
                count = 0
                numBucket = numBucket + 1
        i = i + 1

    if(count > 0): #last row
        #build BloomFilter pattern
        buildBF(listID, count, output)
        #3. Output the BloomFirter pattern to files
        writeBF(output, foutBF, UNCOMPRESSED)
        print "\t\tNumber tuples of last bucket: " + str(count)
        count = 0
        numBucket = numBucket + 1

    #print "\tTotal buckets: " + str(numBucket)
    print "\tTotal tuples: " + str(index)
    foutBF.close()
    print "Total number of buckets: " + str(numBucket) + "!\n"
    print "Finished!"
    return

def evaluation_1(BFsFile, table, column, value1, value2):
    # 1. Do query on the table1 on column1 -> get the list of IDs
    # 1.1 Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111'")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"
    # 1.2 Do querying
    print "Do querying..."
    query = "select distinct id from " + str(table) + " where " + str(column) + " > " + str(value1)
    query = query + " and " + str(column) + " < " + str(value2) + " order by id;"
    cursor.execute(query)
    data = cursor.fetchall()
    # 1.3 Loading data (ID) to buffer
    total_rows = len(data)
    list_ID = [0 for x in range(total_rows)]
    for i in range(total_rows):
        list_ID[i] = int(data[i][0]) # already ordered by ID
    # 2. Load BF to RAM
    print "Loading BFsFile to RAM..."
    totalBuckets = int(math.ceil((float) (MAX_ROW_DB) / (float)(MAX_NUM_ROW_BUCKET_COLUMNAR)))
    #   BFsMetaData[totalBuckets][MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8]
    BFsMetaData = [[0.0 for x in range(0, MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8)] for y in range(0, totalBuckets)]
    fin_BFs = open(BFsFile, 'r')
    index = 0
    curBF = 0 # BF
    for line in fin_BFs:
        if (line.strip() != ''):
            curValue = int(line)
            BFsMetaData[index][curBF] = curValue
            curBF += 1
            if(curBF >= MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8):
                curBF = 0
                index += 1
    fin_BFs.close()
    # 3. Scan for list of buckets of table2.
    print "Scanning for the list of buckets..."
    result = []
    count = 0
    print len(list_ID)
    print totalBuckets
    for i in range(totalBuckets):
        if(i % 1000 == 0):
            print i
        if(checkBF(BFsMetaData[i], list_ID)): # check whether an BF
            result.append(i)
    # 3.1 Printing output ...
    print "Output: "
    print "Total number of IDs (in " + str(column) + "): " + str(len(list_ID)) + "!"
    print "Total number of buckets: " + str(len(result))
    #print "Results: "
    #print result
    print "===================================="
    # 3.1 Printing output ...
    #print "Results: "
    #print result
    return

def evaluation_2(BFsFile, table, column, value1, value2):
    #Limit 10
    # 1. Do query on the table1 on column1 -> get the list of IDs
    # 1.1 Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111'")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"
    # 1.2 Do querying
    print "Do querying..."
    query = "select distinct id from " + str(table) + " where " + str(column) + " > " + str(value1)
    query = query + " and " + str(column) + " < " + str(value2) + " order by id limit 50;"
    cursor.execute(query)
    data = cursor.fetchall()
    # 1.3 Loading data (ID) to buffer
    total_rows = len(data)
    list_ID = [0 for x in range(total_rows)]
    for i in range(total_rows):
        list_ID[i] = int(data[i][0]) # already ordered by ID
    # 2. Load BF to RAM
    print "Loading BFsFile to RAM..."
    totalBuckets = int(math.ceil((float) (MAX_ROW_DB) / (float)(MAX_NUM_ROW_BUCKET_COLUMNAR)))
    #   BFsMetaData[totalBuckets][MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8]
    BFsMetaData = [[0.0 for x in range(0, MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8)] for y in range(0, totalBuckets)]
    fin_BFs = open(BFsFile, 'r')
    index = 0
    curBF = 0 # BF
    for line in fin_BFs:
        if (line.strip() != ''):
            curValue = int(line)
            BFsMetaData[index][curBF] = curValue
            curBF += 1
            if(curBF >= MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8):
                curBF = 0
                index += 1
    fin_BFs.close()
    # 3. Scan for list of buckets of table2.
    print "Scanning for the list of buckets..."
    result = []
    count = 0
    print len(list_ID)
    print totalBuckets
    for i in range(totalBuckets):
        #if(i % 1000 == 0):
        #    print i
        if(checkBF(BFsMetaData[i], list_ID)): # check whether an BF
            result.append(i)
    # 3.1 Printing output ...
    print "Output: "
    print "Total number of IDs (in " + str(column) + "): " + str(len(list_ID)) + "!"
    print "Total number of buckets: " + str(len(result))
    #print "Results: "
    #print result
    print "===================================="
    return

def evaluation_2_2(BFsFile, table, column, value1, value2, limit = -1, ouput = "output.txt"):
    #Limit 10
    # 1. Do query on the table1 on column1 -> get the list of IDs
    # 1.1 Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111'")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"
    # 1.2 Do querying
    print "Do querying..."
    query = "select distinct id from " + str(table) + " where " + str(column) + " > " + str(value1)
    query = query + " and " + str(column) + " < " + str(value2) + " order by id "
    if(limit > 0):
        query = query + " limit " + str(limit)

    cursor.execute(query)
    data = cursor.fetchall()
    # 1.3 Loading data (ID) to buffer
    total_rows = len(data)
    list_ID = [0 for x in range(total_rows)]
    for i in range(total_rows):
        list_ID[i] = int(data[i][0]) # already ordered by ID
    # 2. Load BF to RAM
    print "Loading BFsFile to RAM..."
    totalBuckets = int(math.ceil((float) (MAX_ROW_DB) / (float)(MAX_NUM_ROW_BUCKET_COLUMNAR)))
    #   BFsMetaData[totalBuckets][MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8]
    BFsMetaData = [[0.0 for x in range(0, MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8)] for y in range(0, totalBuckets)]
    fin_BFs = open(BFsFile, 'r')
    index = 0
    curBF = 0 # BF
    for line in fin_BFs:
        if (line.strip() != ''):
            curValue = int(line)
            BFsMetaData[index][curBF] = curValue
            curBF += 1
            if(curBF >= MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8):
                curBF = 0
                index += 1
    fin_BFs.close()
    # 3. Scan for list of buckets of table2.
    print "Scanning for the list of buckets..."
    result = []
    count = 0
    print len(list_ID)
    print totalBuckets
    for i in range(totalBuckets):
        #if(i % 1000 == 0):
        #    print i
        if(checkBF_2(BFsMetaData[i], list_ID)): # check whether an BF
            result.append(i)
    # 3.1 Printing output ...
    print "Output: "
    print "Total number of IDs (in " + str(column) + "): " + str(len(list_ID)) + "!"
    print "Total number of buckets: " + str(len(result))
    #print "Results: "
    #print result
    print "===================================="
    fout = open(output, 'w')
    #write to output file
    fout.close()
    return


def evaluation_3_2(BFsFile, table, column, value1, value2, limit = -1, ouput = "output.txt"):
    #Limit 10
    # 1. Do query on the table1 on column1 -> get the list of IDs
    # 1.1 Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111'")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"
    # 1.2 Do querying
    print "Do querying..."
    query = "select distinct id from " + str(table) + " where " + str(column) + " > " + str(value1)
    query = query + " and " + str(column) + " < " + str(value2) + " order by id "
    if(limit > 0):
        query = query + " limit " + str(limit)

    cursor.execute(query)
    data = cursor.fetchall()
    # 1.3 Loading data (ID) to buffer
    total_rows = len(data)
    list_ID = [0 for x in range(total_rows)]
    for i in range(total_rows):
        list_ID[i] = int(data[i][0]) # already ordered by ID
    # 2. Load BF to RAM
    print "Loading BFsFile to RAM..."
    totalBuckets = int(math.ceil((float) (MAX_ROW_DB) / (float)(MAX_NUM_ROW_BUCKET_COLUMNAR)))
    #   BFsMetaData[totalBuckets][MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8]
    BFsMetaData = [[0.0 for x in range(0, MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8)] for y in range(0, totalBuckets)]
    fin_BFs = open(BFsFile, 'r')
    index = 0
    curBF = 0 # BF
    for line in fin_BFs:
        if (line.strip() != ''):
            curValue = int(line)
            BFsMetaData[index][curBF] = curValue
            curBF += 1
            if(curBF >= MAX_NUM_ROW_BUCKET_COLUMNAR/MAX_BF_PAGE + 8):
                curBF = 0
                index += 1
    fin_BFs.close()
    # 3. Scan for list of buckets of table2.
    print "Scanning for the list of buckets..."
    result = []
    count = 0
    print len(list_ID)
    print totalBuckets
    for i in range(totalBuckets):
        #if(i % 1000 == 0):
        #    print i
        if(checkBF_3(BFsMetaData[i], list_ID)): # check whether an BF
            result.append(i)
    # 3.1 Printing output ...
    print "Output: "
    print "Total number of IDs (in " + str(column) + "): " + str(len(list_ID)) + "!"
    print "Total number of buckets: " + str(len(result))
    #print "Results: "
    #print result
    print "===================================="
    #fout = open(output, 'w')
    #write to output file
    #fout.close()
    return

if __name__ == '__main__':
    #buildBloomFilter_FromDB('trips_proj5_bf.dat', 0)
    #buildBloomFilter_FromDB('trips_proj6_bf.dat', 1)
    #buildBloomFilter_FromDB('trips_proj7_bf.dat', 2)
    #evaluation_1("BFs/trips_proj7_bf.dat", "trips5", "trip_distance", 0.13, 0.15)
    #evaluation_2("BFs/trips_proj7_bf.dat", "trips5", "trip_distance", 0.3, 0.35)
    #buildBloomFilter_FromDB_2('trips_proj5_bf_2.dat', 0)
    #buildBloomFilter_FromDB_2('trips_proj6_bf_2.dat', 1)
    #buildBloomFilter_FromDB_2('trips_proj7_bf_2.dat', 2)
    #evaluation_2_2("BFs/trips_proj7_bf_2.dat", "trips5", "trip_distance", 0.33, 0.35)
    #buildBloomFilter_FromDB_3('trips_proj5_bf_3.dat', 0)
    #buildBloomFilter_FromDB_3('trips_proj6_bf_3.dat', 1)
    #buildBloomFilter_FromDB_3('trips_proj7_bf_3.dat', 2)
    evaluation_3_2("BFs/trips_proj7_bf_3.dat", "trips5", "trip_distance", 0.33, 0.36, -1)
    evaluation_3_2("BFs/trips_proj7_bf_3.dat", "trips5", "trip_distance", 0.33, 0.37, -1)
    print "-----------------------\n"
