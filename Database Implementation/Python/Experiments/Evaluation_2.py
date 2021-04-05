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
import re
from decimal import *

def run_test2(output, inputNum=0):
    #Test IB-Tree
    #1. Read data from listBuckets_sorted_2.txt
    #2. Insert buckets into IB-Tree
    #3. Print IB-Tree

    #1. Read data from listBuckets.txt
    anEntry = IBEntry()
    print "Reading data and inserting into IB-Tree..."
    fin = open("listBuckets_sorted_2.txt", "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    if(inputNum <= 0):
        number = input('Enter a number: ')
    else:
        number = inputNum
    for line in fin:
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
    fin.close()
    print "Number of buckets: " + str(bucketID + 1)
    #Test the search
    listBuckets = ListBuckets()
    tree.search(listBuckets, [0, 0.15])
    print "Result: ", listBuckets.results
    #numScannedEntries = tree.getSE()
    #print "Scanned Entries: ", numScannedEntries
    tree.printEvalInfo()
    tree.evaluation.printEvalInfoToFile(output)
    print "Finished!"
    return

def run_test3(output, inputNum=0):
    #Test IB-Tree
    #1. Read data from listBuckets_sorted_2.txt
    #2. Insert buckets into IB-Tree
    #3. Print IB-Tree

    #1. Read data from listBuckets.txt
    anEntry = IBEntry()
    print "Reading data and inserting into IB-Tree..."
    fin = open("listBuckets_sorted_2.txt", "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    if(inputNum <= 0):
        number = input('Enter a number: ')
    else:
        number = inputNum
    for line in fin:
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
    fin.close()
    print "Number of buckets: " + str(bucketID + 1)
    #Test the search
    listBuckets = ListBuckets()
    tree.search(listBuckets, [0.15, 0.35])
    print "Result: ", listBuckets.results
    #numScannedEntries = tree.getSE()
    #print "Scanned Entries: ", numScannedEntries
    tree.printEvalInfo()
    tree.evaluation.printEvalInfoToFile(output)
    print "Finished!"
    return

def run_test1(output):
    #Test IB-Tree
    #1. Read data from listBuckets_sorted.txt
    #2. Insert buckets into IB-Tree
    #3. Print IB-Tree

    #1. Read data from listBuckets.txt
    anEntry = IBEntry()
    print "Reading data and inserting into IB-Tree..."
    fin = open("listBuckets_sorted.txt", "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    number = input('Enter a number: ')
    for line in fin:
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
    fin.close()
    print "Number of buckets: " + str(bucketID + 1)
    #Test the search
    listBuckets = ListBuckets()
    tree.search(listBuckets, [0, 0.15])
    print "Result: ", listBuckets.results
    #numScannedEntries = tree.getSE()
    #print "Scanned Entries: ", numScannedEntries
    tree.printEvalInfo()
    tree.evaluation.printEvalInfoToFile(output)
    print "Finished!"
    return

def run_test():
    print intersect([0.15, 0.35], [0.14, 0.14])
if __name__ == '__main__':
    #IB-Tree
    #run_test1(./ExperimentalResults/IBTree_Test1.txt)
    #[0.0, 0.15]
    run_test2("./ExperimentalResults/IBTree_Test2_1.txt", 500) # 500
    run_test2("./ExperimentalResults/IBTree_Test2_2.txt", 5000) # 5 000
    run_test2("./ExperimentalResults/IBTree_Test2_3.txt", 10000) # 10 000
    run_test2("./ExperimentalResults/IBTree_Test2_4.txt", 20000) # 20 000
    run_test2("./ExperimentalResults/IBTree_Test2_5.txt", 50000) # 50 000
    #[0.15, 0.35]
    run_test3("./ExperimentalResults/IBTree_Test3_1.txt", 500) # 500
    run_test3("./ExperimentalResults/IBTree_Test3_2.txt", 5000) # 5 000
    run_test3("./ExperimentalResults/IBTree_Test3_3.txt", 10000) # 10 000
    run_test3("./ExperimentalResults/IBTree_Test3_4.txt", 20000) # 20 000
    run_test3("./ExperimentalResults/IBTree_Test3_5.txt", 50000) # 50 000
    #run_test()