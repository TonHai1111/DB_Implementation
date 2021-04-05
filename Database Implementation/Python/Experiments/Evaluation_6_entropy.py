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
import sys

MAX_NUM_ROW_BUCKET = 20
MAX_DISTANCE = 999999.0
MIN_DISTANCE = 0.0


def calculate_entropy(interval, numRows):
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    strQuery = "select count(*) from (select * from trips where id >= 1 and id <=" + str(numRows) + ") as t1 where "

    sValue = interval[0]
    eValue = interval[0]
    Pi = [0 for x in range(0, int((interval[1] - interval[0])/0.1))]
    index = 0
    total = 0
    while(eValue < interval[1]):
        sValue = eValue
        eValue += 0.1
        query = strQuery + "t1.trip_distance >= " + str(sValue)
        query += " and t1.trip_distance < " + str(eValue) + ";"
        cursor.execute(query)
        data = cursor.fetchall()
        for row in data:
            Pi[index] = int(row[0])
            total += int(row[0])
            print "Index: " + str(index) + " | Value: " + str(int(row[0]))
            index += 1
            break
    print "Total: " + str(total)
    print "Finished!"

def calculate_entropy_improved(interval, numRows):
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    strQuery = "select t1.trip_distance, t1.id from (select * from trips where id >= " + str(numRows[0])
    strQuery += " and id <=" + str(numRows[1]) + ") as t1 where "

    query = strQuery + "t1.trip_distance >= " + str(interval[0])
    query += " and t1.trip_distance < " + str(interval[1]) + ";"

    Pi = [0 for x in range(0, int((interval[1] - interval[0])/0.1))]
    index = 0
    total = 0
    cursor.execute(query)
    data = cursor.fetchall()
    value = 0.0
    total1 = 0
    for row in data:
        value = float(row[0])
        cValue = math.ceil(value/0.1)
        if(int(value) == int(cValue)):
            index = cValue
        else:
            index = cValue - 1
        Pi[int(index)] += 1
        total += 1
    Entropy = 0.0
    for i in range(0, int((interval[1] - interval[0])/0.1)):
        #print "Index: " + str(i) + " | Value: " + str(Pi[i])
        total1 += Pi[i] # to verify
        #Entropy
        if(Pi[i] > 0):
            Entropy += (-1.0)*(float(Pi[i])/float(total))*float((math.log((float(Pi[i])/float(total)), 400)))


    #print "Total: " + str(total) + " | total1: " + str(total1)
    print "Entropy: " + str(Entropy)

    foutput = open("DataDistribution_NYC.dat", "w+")
    strWrite = "Data distribution NYC Taxi Dataset\n"
    for i in range(len(Pi)):
        strWrite += str(Pi) + "\n"
    strWrite += "Total: " + str(total)
    foutput.write(strWrite)
    foutput.close()

    print "Finished!"

def calculate_Spearman_correlation_improved(numRows):
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    strQuery = "select trip_distance from trips where id >=" + str(numRows[0]) + " and id <" + str(numRows[1])
    Xi = [0.0 for x in range(0, int(numRows[1] - numRows[0]))]
    cursor.execute(strQuery)
    data = cursor.fetchall()
    count = 0
    totalX = 0.0
    for row in data:
        value = float(row[0])
        Xi[count] = value
        count += 1
        totalX += value
    AvgX = totalX / float(count)
    AvgY = float(count) / 2.0
    SumXY = 0.0
    SumX2 = 0.0
    SumY2 = 0.0
    for i in range(0, count):
        SumXY += (Xi[i] - AvgX) * (i - AvgY)
        SumX2 += (Xi[i] - AvgX) * (Xi[i] - AvgX)
        SumY2 += (i - AvgY) * (i - AvgY)

    Spearman_correlation = SumXY / math.sqrt(SumX2 * SumY2)
    print "Spearman's correlation: " + str(Spearman_correlation)
    print "Finished!"

def binarySort(ListValue, RankValue, start, end):
    if(start >= end):
        return
    if(start == (end - 1)):
        if(ListValue[end] < ListValue[start]):
            temp = ListValue[start]
            ListValue[start] = ListValue[end]
            ListValue[end] = temp
            #Rank
            tempV = RankValue[start]
            RankValue[start] = RankValue[end]
            RankValue[end] = tempV
        return

    mid = int((end + start)/2)
    #Move mid to top
    temp = ListValue[mid]
    ListValue[mid] = ListValue[start]
    ListValue[start] = temp

    #Rank
    tempV = RankValue[mid]
    RankValue[mid] = RankValue[start]
    RankValue[start] = tempV

    sort = start
    for i in range(start + 1, end + 1):
        if(ListValue[i] < ListValue[start]):
            sort += 1
            temp = ListValue[sort]
            ListValue[sort] = ListValue[i]
            ListValue[i] = temp
            #Rank
            tempV = RankValue[sort]
            RankValue[sort] = RankValue[i]
            RankValue[i] = tempV
    binarySort(ListValue, RankValue, start, sort)
    binarySort(ListValue, RankValue, sort + 1, end)
    return

def binarySort2(ListValue, start, end):
    if(start >= end):
        return
    if(start == (end - 1)):
        if(ListValue[end] < ListValue[start]):
            temp = ListValue[start]
            ListValue[start] = ListValue[end]
            ListValue[end] = temp
        return

    mid = int((end + start)/2)
    #Move mid to top
    temp = ListValue[mid]
    ListValue[mid] = ListValue[start]
    ListValue[start] = temp

    sort = start
    for i in range(start + 1, end + 1):
        if(ListValue[i] < ListValue[start]):
            sort += 1
            temp = ListValue[sort]
            ListValue[sort] = ListValue[i]
            ListValue[i] = temp
    binarySort2(ListValue, start, sort)
    binarySort2(ListValue, sort + 1, end)
    return

def insertionSort(ListValue, RankValue, start, end):
    i = start
    while (i < (end + 1)):
        j = i
        while ((j > 0) & (ListValue[j - 1] > ListValue[j])):
            #Value
            temp = ListValue[j - 1]
            ListValue[j - 1] = ListValue[j]
            ListValue[j] = temp
            #Rank
            tempR = RankValue[j - 1]
            RankValue[j - 1] = RankValue[j]
            RankValue[j] = tempR
            j -= 1
        i += 1
    return

def calculate_Spearman_correlation_improved_no_tired_rank(numRows):
    sys.setrecursionlimit(1000000)
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    strQuery = "select trip_distance from trips where id >=" + str(numRows[0]) + " and id <" + str(numRows[1])
    Xi = [0.0 for x in range(0, int(numRows[1] - numRows[0]))]
    Ranki = [0 for x in range(0, int(numRows[1] - numRows[0]))]
    cursor.execute(strQuery)
    data = cursor.fetchall()
    count = 0
    data1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    data1 = [10, 8, 9, 7, 2, 4, 1, 3, 6, 5]
    data2 = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    for row in data:
    #for i in range(0, 10):
        #value = float(data1[i])
        value = float(row[0])
        Xi[count] = value
        Ranki[count] = count + 1
        count += 1
    print count
    binarySort(Xi, Ranki, 0, int(count - 1))

    Sumd2 = 0.0
    for i in range(0, count):
        Sumd2 += (Ranki[i] - i - 1)*(Ranki[i] - i - 1)


    Spearman_correlation = 1 - (6.0 * Sumd2) / (count * (count * count -1))
    print "Spearman's correlation: " + str(Spearman_correlation)
    print "Finished!"

def calculate_Spearman_correlation_improved_BD(numRows):
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='BigDataTest' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    strQuery = "select pt from muon where ctid >= \'" + str(numRows[0]) + "\' and ctid <\'" + str(numRows[1]) + "\'"
    Xi = [0.0 for x in range(0, 1100000)]
    cursor.execute(strQuery)
    data = cursor.fetchall()
    count = 0
    totalX = 0.0
    for row in data:
        value = float(row[0])
        Xi[count] = value
        count += 1
        totalX += value
    AvgX = totalX / float(count)
    AvgY = float(count) / 2.0
    SumXY = 0.0
    SumX2 = 0.0
    SumY2 = 0.0
    for i in range(0, count):
        SumXY += (Xi[i] - AvgX) * (i - AvgY)
        SumX2 += (Xi[i] - AvgX) * (Xi[i] - AvgX)
        SumY2 += (i - AvgY) * (i - AvgY)

    Spearman_correlation = SumXY / math.sqrt(SumX2 * SumY2)
    print "Spearman's correlation: " + str(Spearman_correlation)
    print "Finished!"

def calculate_entropy_improved_BD(interval, start, end):
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='BigDataTest' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    strQuery = "select t1.pt from (select * from muon where ctid >= \'" + str(start)
    strQuery += "\' and ctid <=\'" + str(end) + "\') as t1 where "

    query = strQuery + "t1.pt >= " + str(interval[0])
    query += " and t1.pt < " + str(interval[1]) + ";"

    Pi = [0 for x in range(0, int((interval[1] - interval[0])))]
    index = 0
    total = 0
    cursor.execute(query)
    data = cursor.fetchall()
    value = 0.0
    total1 = 0
    for row in data:
        value = float(row[0])
        cValue = math.ceil(value)
        if(int(value) == int(cValue)):
            index = cValue
        else:
            index = cValue - 1
        Pi[int(index) - 3] += 1
        total += 1
    Entropy = 0.0
    for i in range(0, int((interval[1] - interval[0]))):
        #print "Index: " + str(i) + " | Value: " + str(Pi[i])
        total1 += Pi[i] # to verify
        #Entropy
        if(Pi[i] > 0):
            Entropy += (-1.0)*(float(Pi[i])/float(total))*float((math.log((float(Pi[i])/float(total)), 400)))


    print "Total: " + str(total) + " | total1: " + str(total1)
    print "Entropy: " + str(Entropy)

    foutput = open("DataDistribution_BD.dat", "w+")
    strWrite = "Data distribution BigData Dataset\n"
    for i in range(len(Pi)):
        strWrite += str(Pi) + "\n"
    strWrite += "Total: " + str(total)
    foutput.write(strWrite)
    foutput.close()

    print "Finished!"

def buildOutputIBPlusLayout(inputNum=0, start=0, inputNum2=0):
    #Test IBPlus-Tree

    #1. Read data from listBuckets_sorted_2.txt
    print "Reading data and inserting into IB-Tree..."
    fin1 = open("listBuckets_sorted_4.txt", "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    if(inputNum <= 0):
        number = input('Enter a number: ')
    else:
        number = inputNum

    numTuple = 0
    min = 0.0
    max = 0.0
    for line in fin1:
        tokens = line.split(' ')
        if(count >= number): #reach limitation of number of bucket
            break
        if(re.match('^#.*$', line)):
            continue
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue
        values = line.rstrip('\n').split(', ')
        value = Decimal(values[12])
        if(numTuple == 0):
            min = max = value
            numTuple += 1
        else:
            if(min > value):
                min = value
            if(max < value):
                max = value
            numTuple += 1
            if(numTuple == 1000):
                interval[0] = min
                interval[1] = max
                tree.insertBucket(interval, bucketID)
                bucketID += 1
                numTuple = 0
                min = max = 0.0
                count += 1
    #3. Print IB-Tree
    print "Done!"
    #print "Printing IB-Tree..."
    #tree.printIBTree()
    fin1.close()
    print "Number of buckets: " + str(bucketID)

    #IB+-Tree
    #4. IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree(tree)
    plusTree.copyStructure(tree)
    ##5. Print structure of IB+-Tree
    #plusTree.printIBPlusTree(False)
    #6. Read tuples from listBuckets_2.txt
    print "Reading data and inserting into IB+-Tree..."
    fin2 = open("listBuckets_random_3.txt", "r")
    count2 = 0
    if(inputNum2 <= 0):
        numberIBPlus = input('Please select number of tuples to be insert into IB+-Tree: ')
    else:
        numberIBPlus = inputNum2
    #7. Insert these tuples into IB+-Tree
    cStart = 0
    for line in fin2:
        tokens = line.split(' ')
        if(count2 >= numberIBPlus):
            break
        if(re.match('^#.*$', line)):
            continue
        if((tokens[0] != "interval") & (tokens[0] != "bucketID:")):
            cStart += 1
            if(cStart < start):
                continue
            values = line.rstrip('\n').split(', ')
            value = Decimal(values[12])
            #tuple = Tuple()
            #tuple.key = value
            #tuple.data = [value]
            plusTree.insertTuple(value, [value])
            count2 += 1

    print "Number of tuples: " + str(count2)
    ## 8. Print structure and all data in IB+-Tree
    #plusTree.printIBPlusTree(True)
    fin2.close()
    plusTree.flush(tree)
    print "Finished!"
    return

def calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree(start, numRows, input):
    sys.setrecursionlimit(1000000)
    print "Reading IBPlusTree layout info from " + str(input) + "..."

    fin = open(input, "r")

    Xi = [0.0 for x in range(0, int(numRows))]
    Ranki = [0 for x in range(0, int(numRows))]
    count = 0
    cStart = 0
    for line in fin:
        if(count >= numRows):
            break
        values = line.split('\'')
        if(len(values)>=3):
            cStart += 1
            if(cStart < start):
                continue
            value = float(values[1])
            Xi[count] = value
            Ranki[count] = count + 1
            count += 1
    fin.close()
    print count
    #print cStart
    #binarySort(Xi, Ranki, 0, int(count - 1))
    insertionSort(Xi, Ranki, 0, int(count - 1))
    print "Finish sorting!"
    Sumd2 = 0.0
    for i in range(0, count):
        Sumd2 += (Ranki[i] - i - 1)*(Ranki[i] - i - 1)

    print "Finish calculating!"
    Spearman_correlation = 1 - (6.0 * Sumd2) / (count * (count * count -1))
    print "Spearman's correlation: " + str(Spearman_correlation)
    print "Finished!"

def calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(start, numRows, input):
    sys.setrecursionlimit(1000000)
    #print "Reading IBPlusTree layout info from " + str(input) + "..."

    fin = open(input, "r")

    Xi = [0.0 for x in range(0, int(numRows))]
    Ranki = [0 for x in range(0, int(numRows))]
    count = 0
    cStart = 0
    for line in fin:
        if (count >= numRows):
            break
        values = line.split('\'')
        if (len(values) >= 3):
            cStart += 1
            if (cStart < start):
                continue
            value = float(values[1])
            Xi[count] = value
            Ranki[count] = count + 1
            count += 1
    fin.close()
    #print Xi
    #print count
    insertionSort(Xi, Ranki, 0, int(count - 1))
    Sumd2 = 0.0
    for i in range(0, count):
        Sumd2 += (Ranki[i] - i - 1) * (Ranki[i] - i - 1)

    #print "Finish calculating!"
    Spearman_correlation = 1 - (6.0 * Sumd2) / (count * (count * count - 1))
    print "Spearman's correlation: " + str(Spearman_correlation)
    #print "Finished!"
    return Spearman_correlation

def calculate_SSE(start, size, numRows, input):
    Xi = [0.0 for x in range(0, int(size))]
    fin = open(input, "r")
    count = 0
    cStart = 0
    index = 0
    sum = 0.0
    SSE = 0.0
    numBucket = 0
    for line in fin:
        if(count >= numRows):
            break
        values = line.split('\'')
        if(len(values) >= 3):
            cStart += 1
            if(cStart < start):
                continue
            value = float(values[1])
            Xi[index] = value
            sum += value
            count += 1
            index += 1
            if(index == int(size)):
                sum = sum / float(size)
                SumErr2 = 0.0
                for j in range(0, size):
                    SumErr2 += (Xi[j] - sum) * (Xi[j] - sum)
                #print SumErr2
                if(SumErr2 < 1000):#Eliminate error samples
                    SSE += SumErr2
                    numBucket += 1
                #else:
                #    print SumErr2
                #if(numBucket % 1000 == 0):
                #    print numBucket
                index = 0
                sum = 0.0
    fin.close()
    SSE = SSE / float(numBucket)
    print SSE
    return

def calculate_ARB(size, numRows, sortedFile, IBTreeFile):
    #Xi = [0.0 for x in range(0, int(size))]

    #Calculate optimal value
    fin1 = open(sortedFile, "r")
    count = 0
    min = 0.0
    max = 0.0
    numTuple = 0
    Range_optimal = 0.0
    for line in fin1:
        if(count >= numRows):
            break
        if(re.match('^#.*$', line)):
            continue
        tokens = line.split(' ')
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue

        values = line.rstrip('\n').split(', ')
        value = float(values[12])
        if(numTuple == 0):
            min = value
            max = value
            numTuple += 1
        else:
            if(min > value):
                min = value
            if(max < value):
                max = value
            numTuple += 1
            if(numTuple == size):
                if(max - min > 1):
                    print line
                Range_optimal += max - min
                numTuple = 0
                min = max = 0.0
                count += 1
    fin1.close()
    print count
    #Calculate real value
    fin2 = open(IBTreeFile, "r")
    count = 0
    min = 0.0
    max = 0.0
    numTupe = 0
    Range_real = 0.0
    for line in fin2:
        if(count >= numRows):
            break
        values = line.split(',')
        if(len(values) >= 2):
            min = float(values[0].replace("[", ""))
            max = float(values[1].replace("]", ""))
            #if(max - min > 100):
            #    print line
            if(max - min > 40):
                print line
                continue
            Range_real += max - min
            min = max = 0.0
            count += 1
    fin2.close()

    print "Optimal value: " + str(Range_optimal) + "; Real value: " + str(Range_real) + "; Count: " + str(count)
    ARB = Range_optimal / Range_real
    print "ARB :" + str(ARB)
    return

def selectSortedData(size, numRows, IBTreeFile):
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    strQuery = "select trip_distance from trips_i where trip_distance >= 0 limit 1000000;"
    strQuery = "select trip_distance from trips_i order by trip_distance;"

    cursor.execute(strQuery)
    data = cursor.fetchall()
    count = 0
    numTuple = 0
    Range_optimal = Decimal(0.0)
    for row in data:
        if (count >= numRows):
            break
        value = Decimal(row[0])
        if(numTuple == 0):
            min = max = value
            numTuple += 1
        else:
            if(min > value):
                min = value
            if(max < value):
                max = value
            numTuple += 1
            if(numTuple == size):
                #if(max - min > 1):
                #    print line
                Range_optimal += Decimal(max - min)
                numTuple = 0
                min = max = 0.0
                count += 1
    #count += 1
    print count
    # Calculate real value
    fin2 = open(IBTreeFile, "r")
    count = 0
    min = 0.0
    max = 0.0
    numTupe = 0
    Range_real = Decimal(0.0)
    for line in fin2:
        if (count >= numRows):
            break
        values = line.split(',')
        if (len(values) >= 2):
            min = Decimal(values[0].replace("[", ""))
            max = Decimal(values[1].replace("]", ""))
            # if(max - min > 100):
            #    print line
            if (max - min > 40):
                print line
                continue
            Range_real += max - min
            min = max = 0.0
            count += 1
    fin2.close()

    print "Optimal value: " + str(Range_optimal) + "; Real value: " + str(Range_real) + "; Count: " + str(count)
    ARB = Range_optimal / Range_real
    print "ARB :" + str(ARB)
    return

def calculate_ARB2(size, numRows, input):
    sys.setrecursionlimit(1000000)
    fin1 = open(input, "r")
    readNum = size * numRows
    Data = [0.0 for x in range(0, readNum)]
    count = 0
    for line in fin1:
        if(count >= readNum):
            break
        values = line.split('\'')
        if(len(values) >= 3):
            value = float(values[1])
            Data[count] = value
            count += 1
    fin1.close()
    print count
    print "Sorting..."
    binarySort2(Data, 0, readNum - 1)
    min = 0.0
    max = 0.0
    Range_optimal = 0.0
    numTuple = 0
    for i in range(0, readNum):
        value = Data[i]
        if(numTuple == 0):
            min = value
            max = value
            numTuple += 1
        else:
            if(min > value):
                min = value
            if(max < value):
                max = value
            numTuple += 1
            if(numTuple == size):
                Range_optimal += max - min
                numTuple = 0
                min = max = 0.0
    fin2 = open(input, "r")
    count = 0
    min = 0.0
    max = 0.0
    Range_real = float(0.0)
    for line in fin2:
        if (count >= numRows):
            break
        values = line.split(',')
        if (len(values) >= 2):
            min = float(values[0].replace("[", ""))
            max = float(values[1].replace("]", ""))
            #if (max - min > 40):
            #    print line
            #    continue
            Range_real += max - min
            min = max = 0.0
            count += 1
    fin2.close()
    print "Optimal value: " + str(Range_optimal) + "; Real value: " + str(Range_real) + "; Count: " + str(count)
    ARB = Range_optimal / Range_real
    print "ARB :" + str(ARB)
    return

def calculateInputData(size, numRows, input):
    fin1 = open(input, "r")
    count = 0
    min = 0.0
    max = 0.0
    numTuple = 0
    Range_real = 0.0
    for line in fin1:
        if(count >= numRows):
            break
        if(re.match('^#.*$', line)):
            continue
        tokens = line.split(' ')
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue

        values = line.rstrip('\n').split(', ')
        value = float(values[12])
        if(numTuple == 0):
            min = value
            max = value
            numTuple += 1
        else:
            if(min > value):
                min = value
            if(max < value):
                max = value
            numTuple += 1
            if(numTuple == size):
                Range_real += max - min
                numTuple = 0
                min = max = 0.0
                count += 1
    fin1.close()
    print "Real value: " + str(Range_real) + "; Count: " + str(count)
    return

if __name__ == '__main__':
    #calculateInputData(250, 4000, "listBuckets_random_3.txt")
    #calculateInputData(500, 2000, "listBuckets_random_3.txt")
    #calculateInputData(1000, 1000, "listBuckets_random_3.txt")
    #calculateInputData(2000, 500, "listBuckets_random_3.txt")

    calculate_ARB2(250, 4000, "ibTreeData_4_250.dat")
    calculate_ARB2(500, 2000, "ibTreeData_4_500.dat")
    calculate_ARB2(1000, 1000, "ibTreeData_4_1000.dat")
    calculate_ARB2(2000, 500, "ibTreeData_4_2000.dat")

    #selectSortedData(250, 1000, "ibTreeData_4_250.dat")
    #selectSortedData(500, 1000, "ibTreeData_4_500.dat")
    #selectSortedData(1000, 1000, "ibTreeData_4_1000.dat")
    #selectSortedData(1000, 1000, "ibTreeData_4_2000.dat")
    #selectSortedData(1000, "ibTreeData_4_4000.dat")

    #calculate_ARB(250, 3000, "listBuckets_sorted_4.txt", "ibTreeData_4_250.dat")
    #calculate_ARB(500, 3000, "listBuckets_sorted_4.txt", "ibTreeData_4_500.dat")
    #calculate_ARB(1000, 3000, "listBuckets_sorted_4.txt", "ibTreeData_4_1000.dat")
    #calculate_ARB(1000, 3000, "listBuckets_sorted_4.txt", "ibTreeData_4_2000.dat")
    #calculate_ARB(2000, 1000, "listBuckets_sorted_4.txt", "ibTreeData_4_4000.dat")

    #buildOutputIBPlusLayout(10000, 0, 2000000)
    #buildOutputIBPlusLayout(10000, 2000000, 2000000)
    #buildOutputIBPlusLayout(10000, 4000000, 2000000)
    #buildOutputIBPlusLayout(10000, 6000000, 2000000)
    #buildOutputIBPlusLayout(10000, 8000000, 2000000)
    #buildOutputIBPlusLayout(10000, 9000000, 2000000)
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(1, 250, "ibTreeData_4_250.dat") #bucketsize: 250
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(251, 250, "ibTreeData_4_250.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(501, 250, "ibTreeData_4_250.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(751, 250, "ibTreeData_4_250.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(1001, 250, "ibTreeData_4_250.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(1251, 250, "ibTreeData_4_250.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(1501, 250, "ibTreeData_4_250.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(1751, 250, "ibTreeData_4_250.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(2001, 250, "ibTreeData_4_250.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(2251, 250, "ibTreeData_4_250.dat")

    #i = 1
    #result = 0.0
    #count = 0
    #while (i < 2000000):
    #    result += calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree_bucket(i, 4000, "ibTreeData_4_4000.dat")
    #    i += 4000
    #    count += 1
    #    if(count % 100 == 0):
    #        print count
    #result = result / float(count)
    #print result
    #calculate_SSE(0, 250, 3000000, "ibTreeData_4_250.dat")
    #calculate_SSE(0, 500, 3000000, "ibTreeData_4_500.dat")
    #calculate_SSE(0, 1000, 3000000, "ibTreeData_4_1000.dat")
    #calculate_SSE(0, 2000, 3000000, "ibTreeData_4_2000.dat")
    #calculate_SSE(0, 4000, 2000000, "ibTreeData_4_4000.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree(0, 1000000, "ibTreeData.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree(1000000, 1000000, "ibTreeData.dat")

    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree(2000000, 1000000, "ibTreeData.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree(3000000, 1000000, "ibTreeData.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree(4000000, 1000000, "ibTreeData.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree(5000000, 1000000, "ibTreeData.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree(6000000, 1000000, "ibTreeData.dat")
    #calculate_Spearman_correlation_improved_no_tired_rank_IBPlusTree(7000000, 1000000, "ibTreeData.dat")

    #buildOutputIBPlusLayout(10000, 3000000)
    #buildOutputIBPlusLayout(10000, 25000000)
    #New York City taxi dataset: Spearman's correlation
    #calculate_Spearman_correlation_improved_no_tired_rank([0, 1000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([1000, 2000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([2000, 3000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([3000, 4000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([4000, 5000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([5000, 6000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([6000, 7000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([7000, 8000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([8000, 9000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([9000, 10000])  # Spearman's correlation:
    #print "---------------------------\n"

    #calculate_Spearman_correlation_improved_no_tired_rank([10000, 11000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([11000, 12000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([12000, 13000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([13000, 14000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([14000, 15000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([15000, 16000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([16000, 17000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([17000, 18000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([18000, 19000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([19000, 20000])  # Spearman's correlation:
    #print "---------------------------\n"
    #calculate_Spearman_correlation_improved_no_tired_rank([0, 1000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([1000000, 2000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([2000000, 3000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([3000000, 4000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([4000000, 5000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([5000000, 6000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([6000000, 7000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([7000000, 8000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([8000000, 9000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([9000000, 10000000])  # Spearman's correlation:
    #print "---------------------------\n"
    #calculate_Spearman_correlation_improved_no_tired_rank([10000000, 11000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([11000000, 12000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([12000000, 13000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([13000000, 14000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([14000000, 15000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([15000000, 16000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([16000000, 17000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([17000000, 18000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([18000000, 19000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved_no_tired_rank([19000000, 20000000])  # Spearman's correlation:

    #calculate_Spearman_correlation_improved([5000000, 6000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([6000000, 7000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([7000000, 8000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([8000000, 9000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([9000000, 10000000])  # Spearman's correlation:


    #calculate_Spearman_correlation_improved([10000000, 11000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([11000000, 12000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([12000000, 13000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([13000000, 14000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([14000000, 15000000])  # Spearman's correlation:

    #print "---------------------------\n"
    #calculate_Spearman_correlation_improved([15000000, 16000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([16000000, 17000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([17000000, 18000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([18000000, 19000000])  # Spearman's correlation:
    #calculate_Spearman_correlation_improved([19000000, 20000000])  # Spearman's correlation:
    #New York City taxi dataset: Entropy
    #calculate_entropy_improved([0, 40], [1, 1000000]) # Entropy: 0.705302738384
    #calculate_entropy_improved([0, 40], [1000001, 2000000]) # Entropy: 0.7043099577
    #calculate_entropy_improved([0, 40], [2000001, 3000000]) # Entropy: 0.704020888652
    #calculate_entropy_improved([0, 40], [3000001, 4000000]) # Entropy: 0.70767236235
    #calculate_entropy_improved([0, 40], [4000001, 5000000]) # Entropy: 0.709110961371
    #calculate_entropy_improved([0, 40], [5000001, 6000000]) # Entropy: 0.710673998095
    #calculate_entropy_improved([0, 40], [6000001, 7000000]) # Entropy: 0.714335662726
    #calculate_entropy_improved([0, 40], [7000001, 8000000]) # Entropy: 0.713934985512
    #calculate_entropy_improved([0, 40], [8000001, 9000000]) # Entropy: 0.71306026911
    #calculate_entropy_improved([0, 40], [9000001, 10000000]) # Entropy: 0.714957617001

    #calculate_entropy_improved([0, 40], [10000001, 11000000]) # Entropy: 0.714700935079
    #calculate_entropy_improved([0, 40], [11000001, 12000000]) # Entropy: 0.715064244255
    #calculate_entropy_improved([0, 40], [12000001, 13000000]) # Entropy: 0.707576725209
    #calculate_entropy_improved([0, 40], [13000001, 14000000]) # Entropy: 0.704631677514
    #calculate_entropy_improved([0, 40], [14000001, 15000000]) # Entropy: 0.699645467579
    #calculate_entropy_improved([0, 40], [15000001, 16000000]) # Entropy: 0.692736185264
    #calculate_entropy_improved([0, 40], [16000001, 17000000]) # Entropy: 0.699663032914
    #calculate_entropy_improved([0, 40], [17000001, 18000000]) # Entropy: 0.699909365211
    #calculate_entropy_improved([0, 40], [18000001, 19000000]) # Entropy: 0.70007702371
    #calculate_entropy_improved([0, 40], [19000001, 20000000]) # Entropy: 0.700632625401

    #BigData dataset (HEP): Entropy
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

    # BigData dataset: (HEP) Spearman's correlation
    #calculate_Spearman_correlation_improved_BD(["(0,1)", "(30303, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(30303,2)", "(60606, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(60606,2)", "(90909, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(90909,2)", "(121212, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(121212,2)", "(151515, 1)"]) # Spearman's correlation:
    #print "---------------------------\n"
    #calculate_Spearman_correlation_improved_BD(["(151515,2)", "(181818, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(15151,1)", "(45454, 30)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(45454,1)", "(75757, 30)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(75757,1)", "(106060, 30)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(106060,1)", "(136363, 30)"]) # Spearman's correlation:

    #print "---------------------------\n"
    #calculate_Spearman_correlation_improved_BD(["(136363,1)", "(166666, 30)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(161666,1)", "(191931, 27)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(10101,1)", "(40404, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(40404,1)", "(70707, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(70707,1)", "(101010, 1)"]) # Spearman's correlation:

    #print "---------------------------\n"
    #calculate_Spearman_correlation_improved_BD(["(101010,1)", "(131313, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(131313,1)", "(161616, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(161616,1)", "(191919, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(20202,1)", "(50505, 1)"]) # Spearman's correlation:
    #calculate_Spearman_correlation_improved_BD(["(50505,1)", "(80808, 1)"]) # Spearman's correlation:

    print "---------------------------\n"