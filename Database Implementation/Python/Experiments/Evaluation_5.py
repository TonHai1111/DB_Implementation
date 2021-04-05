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

def insertBucketData(filename, outputFile):
    #0. Connect to DB
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='BigDataTest' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"
    #1. Read data from input file
    print "Reading and inserting data into database..."
    finput = open(filename, 'r')
    eCounter = 0
    data = ""
    bucketID = 0
    for line in finput:
        tokens = line.split(' ')
        if ((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue
        if (re.match('^#.*$', line)):
            continue
        data += line
        eCounter += 1
        if (eCounter >= MAX_NUM_ROW_BUCKET):
            #2. Insert BucketData into MuonBucket Table
            insQuery = "INSERT INTO MuonBucket (bucketID, bucketData) VALUES (%s, %s)"
            #temp = (bucketID, psycopg2.Binary(data))
            cursor.execute(insQuery, (bucketID, psycopg2.Binary(data)))
            bucketID += 1
            eCounter = 0
            data = ""
            conn.commit()
            if(bucketID % 1000 == 0):
                print bucketID

    finput.close()
    print "Reading BucketID and CTID information from database..."
    #3. Read the BucketID, CTID
    selQuery = "Select bucketID, CTID from MuonBucket;"
    cursor.execute(selQuery)
    selData = cursor.fetchall()
    print "Write map(BucketID, CTID) to output file..."
    #4. Write the map (BucketID, CTID) to file
    foutput = open(outputFile, "w+")
    count = 0
    for row in selData:
        foutput.write("%s\n" % ", ".join(map(str, row)))
        count += 1
    foutput.close()
    print "Number of inserted BucketID: " + str(bucketID)
    print "Number of BucketID read from DB: " + str(count)
    print "Finished!"
    return

def run_test(mapFile, inputfile, inputNum, inputNum2, queryInterval, output):
    print "Loading the mapFile..."
    fin1 = open(mapFile, 'r')
    numBucket = 0
    index = 0
    listCTID = ["" for x in range(0, 60000)]
    for line in fin1:
        values = line.split(',', 1)
        listCTID[index] = values[1]
        index += 1
    fin1.close()

    #1. Read data from inputfile
    print "Reading data and inserting into IB-Tree..."
    fin2 = open(inputfile, "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    if(inputNum <= 0):
        number = input('Enter a number: ')
    else:
        number = inputNum
    for line in fin2:
        tokens = line.split(' ')
        if(tokens[0] == "interval"):
            if(count >= number):
                break
            #bucketID = int(tokens[1].replace(':', ''))
            interval[0] = float(tokens[2])
            interval[1] = float(tokens[3])
            #print "BucketID: " + str(bucketID) + "\t [" + str(interval[0]) + ", " + str(interval[1]) + "]"
            #print "Inserting the bucketID: ", bucketID
            tree.insertBucket(interval, bucketID)
            count += 1
            bucketID += 1
    #3. Print IB-Tree
    print "Done!"
    #print "Printing IB-Tree..."
    #tree.printIBTree()
    fin2.close()
    print "Number of buckets: " + str(bucketID + 1)

    #IB+-Tree
    #4. IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree(tree)
    plusTree.copyStructure(tree)
    plusTree.ibPlusBuffer.setStartPointBucketID(bucketID)
    #5. Print structure of IB+-Tree
    #plusTree.printIBPlusTree(False)
    #6. Read tuples from BigData_listMuonBuckets_100_200_2_1->23.txt
    count2 = 0
    if(inputNum2 <= 0):
        numberIBPlus = input('Please select number of tuples to be insert into IB+-Tree: ')
    else:
        numberIBPlus = inputNum2
    isStop = False
    print "Reading data and inserting into IB+-Tree..."
    for i in range(1, 24):
        filename = "BigData_listMuonBuckets_100_200_2_" + str(i) + ".txt"
        if(isStop):
            break
        fin2 = open(filename, "r")
        #7. Insert these tuples into IB+-Tree
        for line in fin2:
            tokens = line.split(' ')
            if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
                continue
            if(re.match('^#.*$', line)):
                continue
            if(count2 >= numberIBPlus):
                isStop = True
                break
            values1 = line.rstrip('\n').split(', ')
            if(len(values1) < 5):
                #print "##############Warning:", values1
                continue
            for j in range(0, len(values1)):
                if(values1[j].find("u\'pt\'") != -1):
                    temp = values1[j].split(":")
                    key = Decimal(temp[1])
                    break
            plusTree.insertTuple(key, values1)
            #print "Done!"
            count2 += 1
        fin2.close()

    print "Number of tuples: " + str(count2)
    # 8. Print structure and all data in IB+-Tree
    #plusTree.printIBPlusTree(True)
    # 9. Query data for a given interval

    #runQuery(plusTree, tree, [3.0, 3.3675], listCTID, output)
    #runQuery(plusTree, tree, [3.0, 3.735], listCTID, output)
    #runQuery(plusTree, tree, [3.0, 4.47], listCTID, output)
    #runQuery(plusTree, tree, [3.0, 5.94], listCTID, output)
    #runQuery(plusTree, tree, [3.0, 10.35], listCTID, output)
    #10. Connect to DB
    print "Connecting to the database..."
    try:
     conn = psycopg2.connect("dbname='BigDataTest' user='tonhai' password='TonHai1111' host='localhost' ")
     cursor = conn.cursor()
    except:
     print "Error: Cannot connect to the database!"
     return
    print "Connected!"


    listBuckets = ListBuckets()
    listTuples = ListTuples()
    timeCalculator = timer()
    timeCalculator.start()
    plusTree.search(listTuples, listBuckets, queryInterval)
    resultLength = len(listBuckets.results)
    readQuery = ""
    if(resultLength >= 1):
     readQuery = "select * from MuonBucket where "
     tempID = int(listBuckets.results[0])
     tempCTID = listCTID[tempID].rstrip('\n')
     strAdd = "CTID = \'" + str(tempCTID) + "\' "
     readQuery += strAdd
     for i in range(1, resultLength):
         tempID = int(listBuckets.results[i])
         tempCTID = listCTID[tempID].rstrip('\n')
         if(tempCTID == ''):
             continue
         strAdd = "or CTID = \'" + str(tempCTID) + "\' "
         readQuery += strAdd
     cursor.execute(readQuery)
     bufferData = cursor.fetchall()

    timeCalculator.end()

    # 10. Print result
    tempString = "Buckets: " + str(listBuckets.results)
    tempString += "\nList Bucket's length: " + str(resultLength)
    tempString += "\nList Tuple's length: " + str(len(listTuples.results))
    tempString += "\nExecution time (s): " + str(timeCalculator.getResult()) + "\n"
    print tempString
    tree.printEvalInfo()
    tree.evaluation.printEvalInfoToFile(output)

    fout2 = open(output, 'a+')
    fout2.write(tempString)
    fout2.write(readQuery)
    fout2.close()
    print "Finished!"

    return

def runQuery(plusTree, tree, queryInterval, listCTID, outputFile):

    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='BigDataTest' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"


    listBuckets = ListBuckets()
    listTuples = ListTuples()
    timeCalculator = timer()
    timeCalculator.start()
    plusTree.search(listTuples, listBuckets, queryInterval)
    resultLength = len(listBuckets.results)
    if(resultLength >= 1):
        readQuery = "select * from MuonBucket where "
        tempID = int(listBuckets.results[0])
        tempCTID = listCTID[tempID]
        strAdd = "CTID = \'" + str(tempCTID) + "\' "
        readQuery += strAdd
        for i in range(1, resultLength):
            tempID = int(listBuckets.results[i])
            tempCTID = listCTID[tempID]
            strAdd = "or CTID = \'" + str(tempCTID) + "\' "
            readQuery += strAdd
        cursor.execute(readQuery)
        bufferData = cursor.fetchall()

    timeCalculator.end()

    # 10. Print result
    tempString = "\nQuery Interval: [" + str(queryInterval[0]) + ", " + str(queryInterval[1]) + "]"
    #tempString += "\nBuckets: " + str(listBuckets.results)
    tempString += "\nList Bucket's length: " + str(resultLength)
    tempString += "\nExecution time (s): " + str(timeCalculator.getResult()) + "\n"
    print tempString
    tree.printEvalInfo()
    tree.evaluation.printEvalInfoToFile(outputFile)

    fout2 = open(outputFile, 'a+')
    fout2.write(tempString)
    fout2.close()

    return
def checkDatafile(filename):
    #1. Read data from inputfile
    print "Reading data..."
    fin2 = open(filename, "r")
    interval = [0.0 for x in range(2)]
    bucketID = 0
    count = 0
    for line in fin2:
        tokens = line.split(' ')
        if(tokens[0] == "interval"):
            interval[0] = float(tokens[2])
            interval[1] = float(tokens[3])
            count += 1
            bucketID += 1
    print "Done!"
    fin2.close()
    print "Number of buckets: " + str(bucketID)
    return

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

def run_testing1(value):
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    analyzeQuery = "explain (analyze, buffers) select * from trips_bucket where bucketID = 0 "
    normalQuery = "select * from trips_bucket where bucketID = 0 "
    templist = []
    while (len(templist) < value):
        num = randint(1, 147550)
        templist.append(num)
    windows = 5000
    length = len(templist)
    tt = timer()
    tt.start()
    tt.end()
    #counter = 0
    for i in range (0, length):
        strAdd = " or bucketID = " + str(templist[i])
        analyzeQuery += strAdd
        normalQuery += strAdd
        if(((i > 0) and (i % windows == 0)) or (i == length - 1)):
            #counter += 1
            #print counter
            print i
            tt.reStart(tt.getResult())
            cursor.execute(normalQuery)
            data = cursor.fetchall()
            normalQuery = "select * from trips_bucket where bucketID = 0 "
            tt.end()

    #cursor.execute(analyzeQuery)
    print tt.getResultInSecond()
    #print data
    #Connecting
    #to
    #the
    #database...
    #Connected!
    #595.197942972
    #Finished!

    print "Finished!"

def run_testing2(value):
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    analyzeQuery = "explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.5 "
    normalQuery = "select * from trips_cluster where trip_distance >= 1.0 and trip_distance < " + str(value)
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
    print "Finished!"

def run_testing3():
    #Full scan bucket table
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the database!"
        return
    print "Connected!"

    analyzeQuery = "explain (analyze, buffers) select * from trips_bucket where bucketID "
    normalQuery = "select * from trips_bucket where  "
    windows = 150000
    length = 147550
    tt = timer()
    tt.start()
    tt.end()
    #counter = 0
    i = 0
    while (i < length):
        strAdd = " bucketID >= " + str(i) + " and bucketID < " + str(i + windows)
        i += windows
        os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
        os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
        normalQuery += strAdd
        print normalQuery
        tt.reStart(tt.getResult())
        cursor.execute(normalQuery)
        #data = cursor.fetchall()
        normalQuery = "select * from trips_bucket where "
        tt.end()

    #cursor.execute(analyzeQuery)
    print tt.getResultInSecond()
    #print data
    #Connecting
    #to
    #the
    #database...
    #Connected!
    #595.197942972
    #Finished!

    print "Finished!"
#Query:
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.03125
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.0625
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.125
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.25
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 1.5
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 2
# explain (analyze, buffers) select * from trips_cluster where trip_distance >= 1.0 and trip_distance < 3.5

if __name__ == '__main__':
    #IB+-Tree
    #insertBucketData("BigData_listBuckets_sorted_4.txt", "MapFile_CTID_BucketID.txt") #
    #Query_I - 0.25%
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted.txt", 25000, 500000, [3.0, 3.3675], "./ExperimentalResults/IBPlusTree_TestBigData_3.txt") #
    #checkDatafile("BigData_listBuckets_sorted.txt") # 12644 buckets
    #checkDatafile("BigData_listBuckets_sorted_2.txt")  # 27840 buckets
    #checkDatafile("BigData_listBuckets_sorted_3.txt")  # 53077 buckets
    #checkDatafile("BigData_listBuckets_sorted_4.txt")  # 59667 buckets
    #Query_I -> V
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_4.txt", 60000, 1, [3.0, 3.3675], "./ExperimentalResults/IBPlusTree_TestBigData_Query_I_2.txt")
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_4.txt", 60000, 1, [3.0, 3.735],  "./ExperimentalResults/IBPlusTree_TestBigData_Query_II_2.txt")
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_4.txt", 60000, 1, [3.0, 4.47],   "./ExperimentalResults/IBPlusTree_TestBigData_Query_III_2.txt")
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_4.txt", 60000, 1, [3.0, 5.94],   "./ExperimentalResults/IBPlusTree_TestBigData_Query_IV_2.txt")
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_4.txt", 60000, 1, [3.0, 10.35],  "./ExperimentalResults/IBPlusTree_TestBigData_Query_V_2.txt")

    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_4.txt", 60000, 500000, [3.0, 3.3675], "./ExperimentalResults/IBPlusTree_TestBigData_Query_I_3.txt")
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_4.txt", 60000, 500000, [3.0, 3.735], "./ExperimentalResults/IBPlusTree_TestBigData_Query_II_3.txt")
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_4.txt", 60000, 500000, [3.0, 4.47], "./ExperimentalResults/IBPlusTree_TestBigData_Query_III_3.txt")
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_4.txt", 60000, 500000, [3.0, 5.94], "./ExperimentalResults/IBPlusTree_TestBigData_Query_IV_3.txt")
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_4.txt", 60000, 500000, [3.0, 10.35], "./ExperimentalResults/IBPlusTree_TestBigData_Query_V_3.txt")
    #run_test("MapFile_CTID_BucketID.txt", "BigData_listBuckets_sorted_2.txt", 25000, 500000, [3.0, 3.735], "./ExperimentalResults/IBPlusTree_TestBigData_Query_II.txt")
    #run_testing()
    #os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    #os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    #print "----------Query 0----------"
    #run_testing1(4180)
    #print "---------------------------\n"
    #os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    #os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    #print "----------Query 1---------"
    #run_testing1(5213)
    #print "---------------------------\n"

    #print "----------Query I----------"
    #run_testing1(9899)
    #print "---------------------------\n"
    #print "----------Query II---------"
    #run_testing1(16307)
    #print "---------------------------\n"
    #print "----------Query III--------"
    #run_testing1(28220)
    #print "---------------------------\n"
    #print "----------Query IV---------"
    #run_testing1(48251)
    #print "---------------------------\n"
    #print "----------Query V----------"
    #run_testing1(79146)
    #print "---------------------------\n"
    #run_testing3()
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    print "----------Query 0---------"
    run_testing2(1.03125)
    print "---------------------------\n"
    os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    print "----------Query 1---------"
    run_testing2(1.0625)
    print "---------------------------\n"
    #run_testing3()
    #os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    #os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    #print "----------Query I---------"
    #run_testing2(1.125)
    #print "---------------------------\n"
    #os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    #os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    #print "----------Query II---------"
    #run_testing2(1.25)
    #print "---------------------------\n"
    #os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    #os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    #print "----------Query III--------"
    #run_testing2(1.5)
    #print "---------------------------\n"
    #os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    #os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    #print "----------Query IV---------"
    #run_testing2(2)
    #print "---------------------------\n"
    #os.system("sync; sudo sh -c \'echo 1 > /proc/sys/vm/drop_caches\'")
    #os.system("sync; sudo sh -c \'echo 2 > /proc/sys/vm/drop_caches\'")
    #print "----------Query V----------"
    #run_testing2(3.5)
    #print "---------------------------\n"