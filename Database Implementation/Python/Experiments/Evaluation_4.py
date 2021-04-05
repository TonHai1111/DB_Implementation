#!/usr/bin/python

#TODO:
# 1. Read data from BigData_listBuckets_sorted.txt
# 2. Insert data to IB-Tree
# 3. Read data from BigData_listMuonBuckets_100_200_2_5->10.txt
# 4. Insert data into IBPlus-Tree (-> IB-Tree)
# 5. Do some queries
# 6. Obtain the results

from IBTree import IBTree
from PLITypes.IBEntry import *
from PLITypes.dataTypes import *
from tools.operations import *
from IBPlusTree import IBPlusTree
import re
from decimal import *

def run_test(output, queryInterval, inputNum=50000, inputNum2=50000):
    #Test IBPlus-Tree

    #1. Read data from BigData_listBuckets_sorted.txt
    print "Reading data and inserting into IB-Tree..."
    fin1 = open("BigData_listBuckets_sorted.txt", "r")
    #2. Insert buckets into IB-Tree
    interval = [0.0 for x in range(2)]
    bucketID = 0
    tree = IBTree()
    count = 0
    if(inputNum <= 0):
        number = input('Select the number of buckets to be inserted into IB-Tree: ')
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
    #6. Read tuples from BigData_listMuonBuckets_100_200_2_5->10.txt
    print "Reading data and inserting into IB+-Tree..."
    count2 = 0
    if(inputNum2 <= 0):
        numberIBPlus = input('Please select number of tuples to be insert into IB+-Tree: ')
    else:
        numberIBPlus = inputNum2
    for j in range(5, 11):
        filename = "BigData_listMuonBuckets_100_200_2_" + str(j) + ".txt"
        fin2 = open(filename, "r")
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
            for i in range(0, len(values)):
                if(values[i].find("u\'pt\'") != -1):
                    temp = values[i].split(":")
                    key = Decimal(temp[1])
                    break
            #key = Decimal(values[12])
            plusTree.insertTuple(key, values)
            count2 += 1
        fin2.close()
    print "Number of tuples: " + str(count2)
    # 8. Print structure and all data in IB+-Tree
    #plusTree.printIBPlusTree(True)
    # 9. Query data for a given interval
    listBuckets = ListBuckets()
    listTuples = ListTuples()
    plusTree.search(listTuples, listBuckets, queryInterval)
    # 10. Print result
    print "Buckets: ", listBuckets.results
    strTemp = ""
    #for i in range(len(listTuples.results)):
    #    strTemp += str(listTuples.results[i][12]) + " "
    #    print "Tuples: ", listTuples.results[i]
    #print "Keys: ", strTemp

    tree.printEvalInfo()
    tree.evaluation.printEvalInfoToFile(output)
    print "Finished!"
    return

if __name__ == '__main__':
    #IB+-Tree
    run_test("./ExperimentalResults/IBPlusTree_TestBigData_1.txt", [3.0, 3.1], 50000, 500000) #
    run_test("./ExperimentalResults/IBPlusTree_TestBigData_2.txt", [5.0, 7.0], 50000, 500000) #
