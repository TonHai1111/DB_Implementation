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
import tools
from IBPlusTree import IBPlusTree
import re
from decimal import *

def run_test1():
    #Test IB-Tree
    #1. Read data from listBuckets.txt
    #2. Insert buckets into IB-Tree
    #3. Print IB-Tree

    #
    #1. Read data from listBuckets.txt
    anEntry = IBEntry()
    print "Reading data and inserting into IB-Tree..."
    fin = open("listBuckets.txt", "r")
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
            print "BucketID: " + str(bucketID) + "\t [" + str(interval[0]) + ", " + str(interval[1]) + "]"
            #print "Inserting the bucketID: ", bucketID
            tree.insertBucket(interval, bucketID)
            count += 1
    #3. Print IB-Tree
    print "Done!"
    print "Printing IB-Tree..."
    tree.printIBTree()
    fin.close()
    print "Finished!"
    return

def run_test2():
    #Test IB-Tree
    #1. Read data from listBuckets_2.txt
    #2. Insert buckets into IB-Tree
    #3. Print IB-Tree

    #1. Read data from listBuckets.txt
    anEntry = IBEntry()
    print "Reading data and inserting into IB-Tree..."
    fin = open("listBuckets_2.txt", "r")
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
    tree.search(listBuckets, [0, 0.5])
    print "Result: ", listBuckets.results
    numScannedEntries = tree.getSE()
    print "Scanned Entries: ", numScannedEntries
    print "Finished!"
    return

def run_test3():
    #Test IB-Tree
    #1. Read data from listBuckets_2.txt
    #2. Insert buckets into IB-Tree
    #3. Print IB-Tree
    #4. IB+-Tree = copy structure of IB-Tree
    #5. Print structure of IB+-Tree

    #1. Read data from listBuckets.txt
    anEntry = IBEntry()
    print "Reading data and inserting into IB-Tree..."
    fin = open("listBuckets_2.txt", "r")
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
    #4. IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree(tree)
    plusTree.copyStructure(tree)
    #5. Print structure of IB+-Tree
    plusTree.printIBPlusTree(False)
    print "Finished!"
    return

def run_test4():
    #Test IB-Tree
    #1. Read data from listBuckets_2.txt
    #2. Insert buckets into IB-Tree
    #3. Print IB-Tree
    #4. IB+-Tree = copy structure of IB-Tree
    #5. Print structure of IB+-Tree
    #6. Continue to read tuples from list_Buckets_2.txt
    #7. Insert these tuples into IB+-Tree
    #8. Print structure and all data in IB+-Tree
    #9. Query data for a given interval
    #10. Print result
    ########################################################
    #1. Read data from listBuckets.txt
    anEntry = IBEntry()
    print "Reading data and inserting into IB-Tree..."
    fin = open("listBuckets_2.txt", "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    countIB = 0
    number = input('Select the number of buckets in IB-Tree: ')
    for line in fin:
        tokens = line.split(' ')
        if(tokens[0] == "interval"):
            if(countIB >= number):
                break
            bucketID = int(tokens[1].replace(':', ''))
            interval[0] = float(tokens[2])
            interval[1] = float(tokens[3])
            #print "BucketID: " + str(bucketID) + "\t [" + str(interval[0]) + ", " + str(interval[1]) + "]"
            #print "Inserting the bucketID: ", bucketID
            tree.insertBucket(interval, bucketID)
            countIB += 1
    #3. Print IB-Tree
    print "Done!"
    print "Printing IB-Tree..."
    tree.printIBTree()
    print "Number of buckets: " + str(bucketID + 1)
    #4. IB+-Tree = copy structure of IB-Tree
    plusTree = IBPlusTree(tree)
    plusTree.copyStructure(tree)
    #5. Print structure of IB+-Tree
    plusTree.printIBPlusTree(False)
    #6. Continue to read tuples from list_Buckets_2.txt
    #7. Insert these tuples into IB+-Tree
    countIBPlus = 0
    numberIBPlus = input('Select the number of tuples to insert into IB+-Tree: ')
    for line in fin: #Continue to read tuples from file
        tokens = line.split(' ')
        if((tokens[0] == "interval") | (tokens[0] == "bucketID:")):
            continue
        if(re.match('^#.*$', line)):
            continue
        if(countIBPlus >= numberIBPlus):
            break
        #print "Line: ", line
        values = line.rstrip('\n').split(', ')
        key = Decimal(values[12])
        print "Key: ", key
        print "Tuple: ", values
        print "Inserting tuple whose key " + str(key) + " into IB+-Tree..."
        plusTree.insertTuple(key, values)
        print "Done!"
        countIBPlus += 1
    print "Number of tuples: " + str(countIBPlus)
    #8. Print structure and all data in IB+-Tree
    plusTree.printIBPlusTree(True)
    #9. Query data for a given interval
    listBuckets = ListBuckets()
    listTuples = ListTuples()
    plusTree.search(listTuples, listBuckets, [0.0, 0.5])
    #10. Print result
    print "Buckets: ", listBuckets.results
    strTemp = ""
    for i in range(len(listTuples.results)):
        strTemp += str(listTuples.results[i][12]) + " "
        print "Tuples: ", listTuples.results[i]
    print "Keys: ", strTemp
    #print "Created Tuples: " + str(plusTree.ibPlusBuffer.createdTuples)
    #print "Released Tuples: " + str(plusTree.ibPlusBuffer.releasedTuples)
    #print "Moved Tuples: " + str(plusTree.ibPlusBuffer.movedTuples)
    fin.close()
    print "Finished!"
    return
if __name__ == '__main__':
    #IB-Tree
    #run_test1()
    #run_test2()

    #IBPlus-Tree
    #run_test3()
    run_test4()