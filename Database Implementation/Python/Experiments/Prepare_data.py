#!/usr/bin/python

#Prepare data for the evaluation
# 1. Extract data from postgres database
# 2. Pre-process this data
# 3. Write data to a file

#Query:
# select id, cab_type_id, vendor_id, trip_distance, fare_amount from trips t where t.id in (select id from trips limit 10000) order by id;
# select * from trips t where t.id in (select id from trips limit 10000) order by id;

#Format data output: (listBuckets.txt)
# bucketID: X
# <20 rows of Bucket's data>
# interval X: [low high]
# <###############################>
import psycopg2
from decimal import *
from PLITypes import Constants
from random import uniform, randint
import sys

#NUM_ROW_PER_BUCKET = 20
#MAX_DISTANCE = 99999.0
#MIN_DISTANCE = 0.0

def prepareData():
    #1. Connect to postgres database (nyc-taxi-data)
    cursor = None
    data = None
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    #2. Run the query
    print "Connected!"
    try:
        print "Executing the query..."
        strQuery = "select * from trips t where t.id in (select id from trips limit 10000) order by id"
        cursor.execute(strQuery)
        data = cursor.fetchall()
    except:
        print "Error: Cannot execute the query!"
        return
    print "Writing data to file..."
    #3. write result to file
    fout = open("listBuckets.txt", "w+")
    row_count = 0
    low = Constants.MAX_DISTANCE
    high = Constants.MIN_DISTANCE
    bucketID = 0
    for row in data:
        distance = Decimal(row[12])
        if(low > distance):
            low = distance
        if(high < distance):
            high = distance
        if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
            fout.write("bucketID: %d\n" % bucketID)
        fout.write("%s\n" % ", ".join(map(str, row)))
        row_count += 1
        if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
            fout.write("interval %d: %f %f\n" % (bucketID, low, high))
            low = Constants.MAX_DISTANCE
            high = Constants.MIN_DISTANCE
            bucketID += 1
            fout.write("############################################\n")
    fout.close()
    print "Finished!"
    return

def prepareData2():
    #1. Connect to postgres database (nyc-taxi-data)
    cursor = None
    data = None
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    #2. Run the query
    print "Connected!"
    try:
        print "Executing the query..."
        strQuery = "select * from trips t where t.id in (select id from trips limit 1000000) order by id"
        cursor.execute(strQuery)
        data = cursor.fetchall()
    except:
        print "Error: Cannot execute the query!"
        return
    print "Writing data to file..."
    #3. write result to file
    fout = open("listBuckets_2.txt", "w+")
    row_count = 0
    low = Constants.MAX_DISTANCE
    high = Constants.MIN_DISTANCE
    bucketID = 0
    for row in data:
        distance = Decimal(row[12])
        if(low > distance):
            low = distance
        if(high < distance):
            high = distance
        if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
            fout.write("bucketID: %d\n" % bucketID)
        fout.write("%s\n" % ", ".join(map(str, row)))
        row_count += 1
        if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
            fout.write("interval %d: %f %f\n" % (bucketID, low, high))
            low = Constants.MAX_DISTANCE
            high = Constants.MIN_DISTANCE
            bucketID += 1
            fout.write("############################################\n")
    fout.close()
    print "Finished!"
    return

def prepareData3():
    #1. Connect to postgres database (nyc-taxi-data)
    cursor = None
    data = None
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    #2. Run the query
    print "Connected!"
    try:
        print "Executing the query..."
        strQuery = "select * from trips t where t.id in (select id from trips limit 10000) order by trip_distance"
        cursor.execute(strQuery)
        data = cursor.fetchall()
    except:
        print "Error: Cannot execute the query!"
        return
    print "Writing data to file..."
    #3. write result to file
    fout = open("listBuckets_sorted.txt", "w+")
    row_count = 0
    low = Constants.MAX_DISTANCE
    high = Constants.MIN_DISTANCE
    bucketID = 0
    for row in data:
        distance = Decimal(row[12])
        if(low > distance):
            low = distance
        if(high < distance):
            high = distance
        if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
            fout.write("bucketID: %d\n" % bucketID)
        fout.write("%s\n" % ", ".join(map(str, row)))
        row_count += 1
        if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
            fout.write("interval %d: %f %f\n" % (bucketID, low, high))
            low = Constants.MAX_DISTANCE
            high = Constants.MIN_DISTANCE
            bucketID += 1
            fout.write("############################################\n")
    fout.close()
    print "Finished!"
    return

def prepareData4():
    #1. Connect to postgres database (nyc-taxi-data)
    cursor = None
    data = None
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    #2. Run the query
    print "Connected!"
    try:
        print "Executing the query..."
        strQuery = "select * from trips t where t.id in (select id from trips limit 1000000) order by trip_distance"
        cursor.execute(strQuery)
        data = cursor.fetchall()
    except:
        print "Error: Cannot execute the query!"
        return
    print "Writing data to file..."
    #3. write result to file
    fout = open("listBuckets_sorted_2.txt", "w+")
    row_count = 0
    low = Constants.MAX_DISTANCE
    high = Constants.MIN_DISTANCE
    bucketID = 0
    for row in data:
        distance = Decimal(row[12])
        if(low > distance):
            low = distance
        if(high < distance):
            high = distance
        if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
            fout.write("bucketID: %d\n" % bucketID)
        fout.write("%s\n" % ", ".join(map(str, row)))
        row_count += 1
        if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
            fout.write("interval %d: %f %f\n" % (bucketID, low, high))
            low = Constants.MAX_DISTANCE
            high = Constants.MIN_DISTANCE
            bucketID += 1
            fout.write("############################################\n")
    fout.close()
    print "Finished!"
    return

def checkData():
    #1. Connect to postgres database (nyc-taxi-data)
    cursor = None
    data = None
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"
    for i in range (0, 1000):
        #2. Run the query
        disLow = i * 0.1
        disHigh = (i + 1) * 0.1
        try:
            print "Executing the query..."
            print "ID: ["+ str(disLow) + ", " + str(disHigh) + ")"
            strQuery = "select count(t.id) from trips t where t.id in (select id from trips where trip_distance >= " + str(disLow) + " and trip_distance < " + str(disHigh) + " limit 10000)"
            cursor.execute(strQuery)
            data = cursor.fetchall()
        except:
            print "Error: Cannot execute the query!"
            return
        for row in data:
            if(int(row[0]) < 10000):
                print "False at " + str(i)
    print "Finished!"


def prepareData5():
    #1. Connect to postgres database (nyc-taxi-data)
    cursor = None
    data = None
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"
    fout = open("listBuckets_random_3.txt", "w+")
    bucketID = 0
    for i in range(0, 10):
        # 2. Run the query
        idLow = i * 1000000
        idHigh = (i + 1) * 1000000 + 1
        try:
            print "Executing the query..."
            print "ID: ["+ str(idLow + 1) + ", " + str(idHigh) + ")"
            strQuery = "select * from trips where id > " + str(idLow) + " and " + " id < " + str(idHigh)
            cursor.execute(strQuery)
            data = cursor.fetchall()
        except:
            print "Error: Cannot execute the query!"
            return

        row_count = 0
        low = Constants.MAX_DISTANCE
        high = Constants.MIN_DISTANCE
        #3. write result to file
        print "Writing data to file..."
        print "ID: ["+ str(idLow + 1) + ", " + str(idHigh) + ")"
        for row in data:
            distance = Decimal(row[12])
            if(low > distance):
                low = distance
            if(high < distance):
                high = distance
            if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
                fout.write("bucketID: %d\n" % bucketID)
            fout.write("%s\n" % ", ".join(map(str, row)))
            row_count += 1
            if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
                fout.write("interval %d: %f %f\n" % (bucketID, low, high))
                low = Constants.MAX_DISTANCE
                high = Constants.MIN_DISTANCE
                bucketID += 1
                fout.write("############################################\n")
    fout.close()
    print "Total number of buckets: " + str(bucketID + 1)
    print "Finished!"
    return

def prepareData6():
    #1. Connect to postgres database (nyc-taxi-data)
    cursor = None
    data = None
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"
    fout = open("listBuckets_sorted_3.txt", "a+")
    bucketID = 27102
    for i in range(0, 20):
        # 2. Run the query
        idLow = i * 1000000
        idHigh = (i + 1) * 1000000 + 1
        disLow = 2 + i * 0.05
        disHigh = 2 + (i + 1) * 0.05

        try:
            print "Executing the query..."
            print "ID: ["+ str(idLow + 1) + ", " + str(idHigh) + ")"
            print "distance: [" + str(disLow) + ", " + str(disHigh) + ")"
            strQuery = "select * from trips where id > " + str(idLow) + " and " + " id < " + str(idHigh) + " and trip_distance >= " + str(disLow) + " and trip_distance < " + str(disHigh) + " order by trip_distance"
            cursor.execute(strQuery)
            data = cursor.fetchall()
        except:
            print "Error: Cannot execute the query!"
            return

        row_count = 0
        low = Constants.MAX_DISTANCE
        high = Constants.MIN_DISTANCE
        #3. write result to file
        print "Writing data to file..."
        print "ID: ["+ str(idLow + 1) + ", " + str(idHigh) + ")"
        print "distance: [" + str(disLow) + ", " + str(disHigh) + ")"
        for row in data:
            distance = Decimal(row[12])
            if(low > distance):
                low = distance
            if(high < distance):
                high = distance
            if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
                fout.write("bucketID: %d\n" % bucketID)
            fout.write("%s\n" % ", ".join(map(str, row)))
            row_count += 1
            if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
                fout.write("interval %d: %f %f\n" % (bucketID, low, high))
                low = Constants.MAX_DISTANCE
                high = Constants.MIN_DISTANCE
                bucketID += 1
                fout.write("############################################\n")
        print bucketID
    fout.close()
    print "Total number of buckets: " + str(bucketID + 1)
    print "Finished!"
    return

def prepareData7(bucket, value):
    #1. Connect to postgres database (nyc-taxi-data)
    cursor = None
    data = None
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"
    fout = open("listBuckets_sorted_4.txt", "a+")
    bucketID = bucket
    for i in range(0, 20):
        # 2. Run the query
        idLow = i * 3000000
        idHigh = (i + 1) * 3000000 + 1
        disLow = value + i * 0.05
        disHigh = value + (i + 1) * 0.05

        try:
            print "Executing the query..."
            print "ID: ["+ str(idLow + 1) + ", " + str(idHigh) + ")"
            print "distance: [" + str(disLow) + ", " + str(disHigh) + ")"
            strQuery = "select * from trips where id > " + str(idLow) + " and " + " id < " + str(idHigh) + " and trip_distance >= " + str(disLow) + " and trip_distance < " + str(disHigh) + " order by trip_distance"
            cursor.execute(strQuery)
            data = cursor.fetchall()
        except:
            print "Error: Cannot execute the query!"
            return

        row_count = 0
        low = Constants.MAX_DISTANCE
        high = Constants.MIN_DISTANCE
        #3. write result to file
        print "Writing data to file..."
        print "ID: ["+ str(idLow + 1) + ", " + str(idHigh) + ")"
        print "distance: [" + str(disLow) + ", " + str(disHigh) + ")"
        for row in data:
            distance = Decimal(row[12])
            if(low > distance):
                low = distance
            if(high < distance):
                high = distance
            if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
                fout.write("bucketID: %d\n" % bucketID)
            fout.write("%s\n" % ", ".join(map(str, row)))
            row_count += 1
            if(row_count % Constants.NUM_ROW_PER_BUCKET == 0):
                fout.write("interval %d: %f %f\n" % (bucketID, low, high))
                low = Constants.MAX_DISTANCE
                high = Constants.MIN_DISTANCE
                bucketID += 1
                fout.write("############################################\n")
        print bucketID
    fout.close()
    print "Total number of buckets: " + str(bucketID + 1)
    print "Finished!"
    return bucketID

def prepareData8(number, limit, prefix):
    #1. Connect to postgres database (nyc-taxi-data)
    cursor = None
    data = None
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    bucketID = 0
    for i in range (0, number):
        randValue = uniform(0, 10)
        readQuery = "select * from trips where trip_distance >= " + str(randValue) + "limit " + str(limit) + ";"
        cursor.execute(readQuery)
        data = cursor.fetchall()
        insertQuery = "INSERT INTO trips_bucket" + str(prefix) + " (bucketID, bucketData) VALUES (%s, %s)"
        temp = ""
        for row in data:
            for j in range(0,len(row)):
                temp += str(row[j])
        cursor.execute(insertQuery, (bucketID, temp))
        bucketID += 1
        conn.commit()
        if(bucketID % 10000 == 0):
            print bucketID

    print "Total number of buckets: " + str(bucketID + 1)
    print "Finished!"
    return bucketID

def run_testing():
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    analyzeQuery = "explain (analyze, buffers) select * from trips limit 10;"
    print analyzeQuery
    cursor.execute(analyzeQuery)
    data = cursor.fetchall()
    print data
    print "Finished!"

if __name__ == '__main__':
    #if __name__ == '__main__':
        #checkData()
        # 10 000 tuples - sorted by ID (500 buckets)
        #prepareData()

        # 1 000 000 tuples - sorted by ID (50 000 buckets) - random distance - 180MB
        #prepareData2()

        # 10 000 tuples - sorted by trip_distance (500 buckets)
        #prepareData3()

        # 1 000 000 tuples -sorted by trip_distance (50 000 buckets) - 180MB
        #prepareData4()

        # 10 000 000 tuples - sorted by ID (500 000 buckets) - Random distance - 2.9GB
        #prepareData5()

        # ..
        #prepareData6()
        #bucket = 0
        #for i in range (0, 30):
        #    bucket = prepareData7(bucket, i) # listbucket_sorted_4.txt - 3 008 320 tuples - sorted by trip_distance (150 416 buckets) - 893MB
    #prepareData8(147551, 1000, "")
    prepareData8(295102, 500, "_500")
    prepareData8(590204, 250, "_250")
    prepareData8(73776, 2000, "_2000")
