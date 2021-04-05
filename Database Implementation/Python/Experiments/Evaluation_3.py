#!/usr/bin/python

#TODO:
# 1. Read data from postgres database
# 2. Insert data to IB-Tree/IBPlus-Tree
# 3. Print the tree
# 4. Do some queries
# 5. Obtain the results

from IBTree import IBTree
from PLITypes.IBEntry import *
from PLITypes.dataTypes import *
from tools.operations import *
from IBPlusTree import IBPlusTree
from tools.timeTools import *
import re
from decimal import *
#import psycopg2
import sys
from itertools import islice

def run_test1(output, inputNum=500, inputNum2=100):
    #Test IBPlus-Tree

    #1. Read data from listBuckets_sorted.txt
    print "Reading data and inserting into IB-Tree..."
    fin1 = open("listBuckets_sorted.txt", "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    if(inputNum <= 0):
        number = input('Enter a number: ')
    else:
        number = inputNum
    for line in fin1:
        tokens = line.split(' ')
        if(tokens[0] == "interval"):
            if(count >= number):
                break
            bucketID = int(tokens[1].replace(':', ''))
            interval[0] = float(tokens[2])
            interval[1] = float(tokens[3])
            tree.insertBucket(interval, bucketID)
            count += 1
    #3. Print IB-Tree
    print "Done!"
    print "Printing IB-Tree..."
    tree.printIBTree()
    fin1.close()
    print "Number of buckets: " + str(bucketID + 1)

    #IB+-Tree
    #4. IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree(tree)
    plusTree.copyStructure(tree)
    #5. Print structure of IB+-Tree
    plusTree.printIBPlusTree(False)
    #6. Read tuples from listBuckets.txt
    print "Reading data and inserting into IB+-Tree..."
    fin2 = open("listBuckets.txt", "r")
    count2 = 0
    if(inputNum2 <= 0):
        numberIBPlus = input('Please select number of tuples to be insert into IB+-Tree: ')
    else:
        numberIBPlus = inputNum2
    #7. Insert these tuples into IB+-Tree
    for line in fin2:
        tokens = line.split(' ')
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue
        if(re.match('^#.*$', line)):
            continue
        if(count2 >= numberIBPlus):
            break
        values = line.rstrip('\n').split(', ')
        key = Decimal(values[12])
        plusTree.insertTuple(key, values)
        count2 += 1

    print "Number of tuples: " + str(count2)
    # 8. Print structure and all data in IB+-Tree
    plusTree.printIBPlusTree(True)
    # 9. Query data for a given interval
    listBuckets = ListBuckets()
    listTuples = ListTuples()
    plusTree.search(listTuples, listBuckets, [0.0, 0.15])
    # 10. Print result
    print "Buckets: ", listBuckets.results
    strTemp = ""
    for i in range(len(listTuples.results)):
        strTemp += str(listTuples.results[i][12]) + " "
        print "Tuples: ", listTuples.results[i]
    print "Keys: ", strTemp
    fin2.close()

    tree.printEvalInfo()
    tree.evaluation.printEvalInfoToFile(output)
    print "Finished!"
    return

def run_test2(output, inputNum=0, inputNum2=0):
    #Test IBPlus-Tree

    #1. Read data from listBuckets_sorted_2.txt
    print "Reading data and inserting into IB-Tree..."
    fin1 = open("listBuckets_sorted_2.txt", "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    if(inputNum <= 0):
        number = input('Enter a number: ')
    else:
        number = inputNum
    for line in fin1:
        tokens = line.split(' ')
        if(tokens[0] == "interval"):
            if(count >= number):
                break
            bucketID = int(tokens[1].replace(':', ''))
            interval[0] = float(tokens[2])
            interval[1] = float(tokens[3])
            tree.insertBucket(interval, bucketID)
            count += 1
    #3. Print IB-Tree
    print "Done!"
    print "Printing IB-Tree..."
    tree.printIBTree()
    fin1.close()
    print "Number of buckets: " + str(bucketID + 1)

    #IB+-Tree
    #4. IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree(tree)
    plusTree.copyStructure(tree)
    #5. Print structure of IB+-Tree
    plusTree.printIBPlusTree(False)
    #6. Read tuples from listBuckets_2.txt
    print "Reading data and inserting into IB+-Tree..."
    fin2 = open("listBuckets_2.txt", "r")
    count2 = 0
    if(inputNum2 <= 0):
        numberIBPlus = input('Please select number of tuples to be insert into IB+-Tree: ')
    else:
        numberIBPlus = inputNum2
    #7. Insert these tuples into IB+-Tree
    for line in fin2:
        tokens = line.split(' ')
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue
        if(re.match('^#.*$', line)):
            continue
        if(count2 >= numberIBPlus):
            break
        values = line.rstrip('\n').split(', ')
        key = Decimal(values[12])
        plusTree.insertTuple(key, values)
        count2 += 1

    print "Number of tuples: " + str(count2)
    # 8. Print structure and all data in IB+-Tree
    #plusTree.printIBPlusTree(True)
    # 9. Query data for a given interval
    listBuckets = ListBuckets()
    listTuples = ListTuples()
    plusTree.search(listTuples, listBuckets, [0.0, 0.15])
    # 10. Print result
    print "Buckets: ", listBuckets.results
    strTemp = ""
    for i in range(len(listTuples.results)):
        strTemp += str(listTuples.results[i][12]) + " "
        print "Tuples: ", listTuples.results[i]
    print "Keys: ", strTemp
    fin2.close()

    tree.printEvalInfo()
    tree.evaluation.printEvalInfoToFile(output)
    print "Finished!"
    return

def run_test3(output, inputNum=0, inputNum2=0):
    #Test IBPlus-Tree

    #1. Read data from listBuckets_sorted_2.txt
    print "Reading data and inserting into IB-Tree..."
    fin1 = open("listBuckets_sorted_2.txt", "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    if(inputNum <= 0):
        number = input('Enter a number: ')
    else:
        number = inputNum
    for line in fin1:
        tokens = line.split(' ')
        if(tokens[0] == "interval"):
            if(count >= number):
                break
            bucketID = int(tokens[1].replace(':', ''))
            interval[0] = float(tokens[2])
            interval[1] = float(tokens[3])
            #print "BucketID: " + str(bucketID) + "\t [" + str(interval[0]) + ", " + str(interval[1]) + "]"
            #print "Inserting the bucketID: ", bucketID
            tree.insertBucket(interval, bucketID)
            count += 1
    #3. Print IB-Tree
    print "Done!"
    print "Printing IB-Tree..."
    tree.printIBTree()
    fin1.close()
    print "Number of buckets: " + str(bucketID + 1)

    #IB+-Tree
    #4. IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree(tree)
    plusTree.copyStructure(tree)
    #5. Print structure of IB+-Tree
    plusTree.printIBPlusTree(False)
    #6. Read tuples from listBuckets_2.txt
    print "Reading data and inserting into IB+-Tree..."
    fin2 = open("listBuckets_2.txt", "r")
    count2 = 0
    if(inputNum2 <= 0):
        numberIBPlus = input('Please select number of tuples to be insert into IB+-Tree: ')
    else:
        numberIBPlus = inputNum2
    #7. Insert these tuples into IB+-Tree
    for line in fin2:
        tokens = line.split(' ')
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue
        if(re.match('^#.*$', line)):
            continue
        if(count2 >= numberIBPlus):
            break
        values = line.rstrip('\n').split(', ')
        key = Decimal(values[12])
        #print "Key: ", key
        #print "Tuple: ", values
        #print "Inserting tuple whose key " + str(key) + " into IB+-Tree..."
        plusTree.insertTuple(key, values)
        #print "Done!"
        count2 += 1

    print "Number of tuples: " + str(count2)
    # 8. Print structure and all data in IB+-Tree
    plusTree.printIBPlusTree(True)
    # 9. Query data for a given interval
    listBuckets = ListBuckets()
    listTuples = ListTuples()
    plusTree.search(listTuples, listBuckets, [0.15, 0.35])
    # 10. Print result
    print "Buckets: ", listBuckets.results
    strTemp = ""
    for i in range(len(listTuples.results)):
        strTemp += str(listTuples.results[i][12]) + " "
        print "Tuples: ", listTuples.results[i]
    print "Keys: ", strTemp
    fin2.close()

    tree.printEvalInfo()
    tree.evaluation.printEvalInfoToFile(output)
    print "Finished!"
    return

def run_test4(inputNum=0, inputNum2=0):
    #Test IBPlus-Tree

    #1. Read data from listBuckets_sorted_2.txt
    print "Reading data and inserting into IB-Tree..."
    fin1 = open("listBuckets_sorted_2.txt", "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    if(inputNum <= 0):
        number = input('Enter a number: ')
    else:
        number = inputNum
    for line in fin1:
        tokens = line.split(' ')
        if(tokens[0] == "interval"):
            if(count >= number):
                break
            bucketID = int(tokens[1].replace(':', ''))
            interval[0] = float(tokens[2])
            interval[1] = float(tokens[3])
            tree.insertBucket(interval, bucketID)
            count += 1
    #3. Print IB-Tree
    print "Done!"
    print "Printing IB-Tree..."
    tree.printIBTree()
    fin1.close()
    print "Number of buckets: " + str(bucketID + 1)

    #IB+-Tree
    #4. IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree(tree)
    plusTree.copyStructure(tree)
    ##5. Print structure of IB+-Tree
    #plusTree.printIBPlusTree(False)
    #6. Read tuples from listBuckets_2.txt
    print "Reading data and inserting into IB+-Tree..."
    fin2 = open("listBuckets_2.txt", "r")
    count2 = 0
    if(inputNum2 <= 0):
        numberIBPlus = input('Please select number of tuples to be insert into IB+-Tree: ')
    else:
        numberIBPlus = inputNum2
    #7. Insert these tuples into IB+-Tree
    for line in fin2:
        tokens = line.split(' ')
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue
        if(re.match('^#.*$', line)):
            continue
        if(count2 >= numberIBPlus):
            break
        values = line.rstrip('\n').split(', ')
        key = Decimal(values[12])
        #print "Key: ", key
        #print "Tuple: ", values
        #print "Inserting tuple whose key " + str(key) + " into IB+-Tree..."
        plusTree.insertTuple(key, values)
        #print "Done!"
        count2 += 1

    print "Number of tuples: " + str(count2)
    ## 8. Print structure and all data in IB+-Tree
    #plusTree.printIBPlusTree(True)

    # 9. Query data for a given interval
    listBuckets1 = ListBuckets()
    listBuckets = ListBuckets()
    listTuples = ListTuples()
    plusTree.search(listTuples, listBuckets, [0, 0.15])
    # 10. Print result
    print "Buckets (IB+-Tree): ", listBuckets.results
    tree.search(listBuckets1, [0, 0.15])
    print "Buckets (IB-Tree): ", listBuckets1.results
    #strTemp = ""
    #for i in range(len(listTuples.results)):
    #    strTemp += str(listTuples.results[i][12]) + " "
    #    #print "Tuples: ", listTuples.results[i]
    #print "Keys: ", strTemp

    fin2.close()

    print "=================<<<<>>>>=================="
    plusTree.writeMetaData()

    plusTree.readMetaData()

    tree.writeMetaData()

    tree.readMetaData()

    #print "Printing IB-Tree..."
    #tree.printIBTree()

    #plusTree.ibTree.rootNode = tree.rootNode
    # 9. Query data for a given interval
    listBuckets1 = ListBuckets()
    listBuckets = ListBuckets()
    listTuples = ListTuples()
    plusTree.search(listTuples, listBuckets, [0, 0.15])
    # 10. Print result
    print "Buckets (IB+-Tree): ", listBuckets.results
    tree.search(listBuckets1, [0, 0.15])
    print "Buckets (IB-Tree): ", listBuckets1.results
    #strTemp = ""
    #for i in range(len(listTuples.results)):
    #    strTemp += str(listTuples.results[i][12]) + " "
    #    #print "Tuples: ", listTuples.results[i]
    #print "Keys: ", strTemp
    #tree.printEvalInfo()
    #tree.evaluation.printEvalInfoToFile(output)
    print "Finished!"
    return

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

    #4. Read data from the database
    #5. Insert data into the IBPlusTree
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
        for row in data:
            strValue = "%s\n" % ", ".join(map(str, row))
            key = float(row[12])
            plusTree.insertTuple(key, strValue)
        start = end
    #5. Write the metadata info file
    plusTree.flush()
    print "=================<<<<>>>>=================="
    plusTree.writeMetaData()

    #plusTree.readMetaData()

    #tree.writeMetaData()
    plusTree.ibTree.writeMetaData()

    #tree.readMetaData()

    return

def test1():
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
    listBuckets1 = ListBuckets()
    listBuckets = ListBuckets()
    listTuples = ListTuples()
    plusTree.search(listTuples, listBuckets, [0, 0.15])
    # 5. Print result
    print "Buckets (IB+-Tree): ", listBuckets.results
    tree.search(listBuckets1, [0, 0.15])
    print "Buckets (IB-Tree): ", listBuckets1.results
    return

def test2(interval):
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
    listBuckets = ListBuckets()
    listTuples = ListTuples()
    tt = timer()
    tt.start()
    plusTree.search(listTuples, listBuckets, interval)
    tt.end()
    # 5. Print result
    print "Buckets (IB+-Tree): ", len(listBuckets.results)
    print "Time1: ", tt.resultInSecond
    #readDB(listBuckets.results, "ibPlusTreeDB.dat")
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

def readDB(listBucket, fileDB):
    sys.setrecursionlimit(1000000)
    #1. Ordering the listBucket
    tt = timer()
    tt.start()
    binarySort(listBucket, 0, len(listBucket) - 1)
    tt.end()
    print "Sorting: ", tt.resultInSecond
    #2. Open file and read data
    fin = open(fileDB, "r")
    #TODO:
    tt.reStart()
    curOffset = 0
    for i in range(0, len(listBucket)):
        #move = listBucket[i] - curOffset
        #fin.seek(move, 1)
        #for j in range(0, Constants.NUM_ROW_PER_BUCKET):
        #    lin = fin.readline()
        #curOffset = fin.tell()

        #fin.seek(listBucket[i])
        #for j in range(0, Constants.NUM_ROW_PER_BUCKET):
        #    lin = fin.readline()
        fin.seek(listBucket[i])
        line = list(islice(fin, Constants.NUM_ROW_PER_BUCKET))
    tt.end()
    fin.close()
    print "Time2: ", tt.resultInSecond
    return

def binarySort(ListValue, start, end):
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

    binarySort(ListValue, start, sort)
    binarySort(ListValue, sort + 1, end)
    return

def run_test():
    print intersect([0.15, 0.35], [0.14, 0.14])
    return

def DBScan(DBFile, MDFile):
    tt = timer()
    tt.start()
    fin = open(DBFile, "r")
    count = 0
    line = fin.readline()
    count += 1
    while(line != ""):
        line = fin.readline()
        count += 1
    fin.close()
    fin1 = open(MDFile, "r")
    count1 = 0
    line = fin1.readline()
    count1 += 1
    while(line != ""):
        line = fin1.readline()
        count1 += 1
    fin1.close()
    tt.end()
    print "Number of lines in DB: ", count
    print "Number of lines in MD: ", count1
    print "Time: ", tt.resultInSecond
    return

if __name__ == '__main__':
    #IB+-Tree
    #[0.0, 0.15]
    #run_test1("./ExperimentalResults/IBPlusTree_Test1.txt", 500, 100)
    #[0.0, 0.15]
    #run_test2("./ExperimentalResults/IBPlusTree_Test2_1.txt", 500, 100) #500
    #run_test2("./ExperimentalResults/IBPlusTree_Test2_2.txt", 5000, 1000)  # 5 000
    #run_test2("./ExperimentalResults/IBPlusTree_Test2_3.txt", 10000, 2000)  # 10 000
    #run_test2("./ExperimentalResults/IBPlusTree_Test2_4.txt", 20000, 4000)  # 20 000
    #run_test2("./ExperimentalResults/IBPlusTree_Test2_5.txt", 50000, 12500)  # 50 000

    #run_test2("./ExperimentalResults/IBPlusTree_Test2_6.txt", 50000, 12500)

    #[0.15, 0.35]
    #run_test3("./ExperimentalResults/IBPlusTree_Test3_1.txt", 500, 100) #500
    #run_test3("./ExperimentalResults/IBPlusTree_Test3_2.txt", 5000, 1000)  # 5 000
    #run_test3("./ExperimentalResults/IBPlusTree_Test3_3.txt", 10000, 2000)  # 10 000
    #run_test3("./ExperimentalResults/IBPlusTree_Test3_4.txt", 20000, 4000)  # 20 000
    #run_test3("./ExperimentalResults/IBPlusTree_Test3_5.txt", 50000, 12500)  # 50 000


    #[0.15, 0.35]
    #run_test3("./ExperimentalResults/IBPlusTree_Test3_6.txt", 500, 2000) #500
    #run_test3("./ExperimentalResults/IBPlusTree_Test3_7.txt", 5000, 20000)  # 5 000
    #run_test3("./ExperimentalResults/IBPlusTree_Test3_8.txt", 10000, 40000)  # 10 000
    #run_test3("./ExperimentalResults/IBPlusTree_Test3_9.txt", 20000, 80000)  # 20 000
    #run_test3("./ExperimentalResults/IBPlusTree_Test3_10.txt", 50000, 250000)  # 50 000

    #run_test4(5000, 1000)

    #run_loadData_DB("listBuckets_sorted_4.txt", 140000, 147550000)
    #run_loadData_DB("listBuckets_sorted_4.txt", 10000, 1000000)
    test1()

    #test2([1, 1.03125]) #Time1:  68.3901810646  Time1:  61.5538899899   Time1:  60.0154259205  4093
    #test2([1, 1.0625]) #Time1:  79.7672719955   Time1:  75.3143758774   Time1:  75.1354739666  5125
    #test2([1, 1.125]) #Time1:  147.655161858    Time1:  144.20512414    Time1:  143.893454075  9807
    #test2([1, 1.25]) #Time1:  244.200461149     Time1:  244.414703846   Time1:  244.008317947  16521
    #test2([1, 1.5]) #Time1:  462.294121027      Time1:  451.18317008    Time1:  451.174984932  30476
    #test2([1, 1.75]) #Time1:  617.02230         Time1:  599.2547099     Time1:  598.971589088  40398

    #intervals = [[1, 1.03125], [1, 1.0625], [1, 1.125], [1, 1.25], [1, 1.5], [1, 1.75]]
    #[1, 1.03125]    #Buckets(IB + -Tree):  4178    #Time1:  44.0685801506
    #[1, 1.0625]    #Buckets(IB + -Tree):  5210    #Time1:  55.3898308277
    #[1, 1.125]    #Buckets(IB + -Tree):  9807    #Time1:  103.313462019
    #[1, 1.25]    #Buckets(IB + -Tree):  16521    #Time1:  176.618077993
    #[1, 1.5]    #Buckets(IB + -Tree):  30476    #Time1:  326.972563982
    #[1, 1.75]    #Buckets(IB + -Tree):  40398    #Time1:  433.318394184

    #Result - 1000
    #[1, 1.03125]    #Buckets(IB + -Tree):  4178    #Time1:  32.9001171589
    #[1, 1.0625]    #Buckets(IB + -Tree):  5210    #Time1:  37.9734489918
    #[1, 1.125]    #Buckets(IB + -Tree):  9807    #Time1:  72.9233291149
    #[1, 1.25]    #Buckets(IB + -Tree):  16521    #Time1:  137.394233942
    #[1, 1.5]    #Buckets(IB + -Tree):  30476    #Time1:  260.865678072
    #[1, 1.75]    #Buckets(IB + -Tree):  40398    #Time1:  347.901168108
    #Result - 500
    #[1, 1.03125]    #Buckets(IB + -Tree):  4178    #Time1:  50.7119090557
    #[1, 1.0625]    #Buckets(IB + -Tree):  5210    #Time1:  60.1420109272
    #[1, 1.125]    #Buckets(IB + -Tree):  9807    #Time1:  127.423007965
    #[1, 1.25]    #Buckets(IB + -Tree):  16521    #Time1:  214.594724894
    #[1, 1.5]    #Buckets(IB + -Tree):  30476    #Time1:  388.487329006
    #[1, 1.75]    #Buckets(IB + -Tree):  40398    #Time1:  518.071049929
    #Result - 250
    #[1, 1.03125]    #Buckets(IB + -Tree):  4178    #Time1:  88.0605819225
    #[1, 1.0625]    #Buckets(IB + -Tree):  5210    #Time1:  116.124083042
    #[1, 1.125]    #Buckets(IB + -Tree):  9807    #Time1:  214.56964612
    #[1, 1.25]    #Buckets(IB + -Tree):  16521    #Time1:  379.644057989
    #[1, 1.5]    #Buckets(IB + -Tree):  30476    #Time1:  666.584073067
    #[1, 1.75]    #Buckets(IB + -Tree):  40398    #Time1:  934.586968899
    #Result - 2000
    # [1, 1.03125]    #Buckets(IB + -Tree):  4178    #Time1:  41.0448269844
    #[1, 1.0625]    #Buckets(IB + -Tree):  5210    #Time1:  54.8205289841
    #[1, 1.125]    #Buckets(IB + -Tree):  9807    #Time1:  107.448454142
    #[1, 1.25]    #Buckets(IB + -Tree):  16521    #Time1:  175.170265913
    #[1, 1.5]    #Buckets(IB + -Tree):  30476    #Time1:  312.314023018
    #[1, 1.75]    #Buckets(IB + -Tree):  40398    #Time1:  413.234505892

    #intervals = [[1, 2], [1, 2.25], [1, 2.5], [1, 2.75], [1, 3], [1, 3.5]]
    #test3(intervals, 6)
    #DBScan("ibPlusTreeDB.dat", "ibPlusTreeMD.dat")
    #147 153 lines in DB, 130 176 lines in MD
    #1132.113(s)
