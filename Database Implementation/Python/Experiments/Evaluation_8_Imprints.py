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
from tools.generalTools import  *
import re
from decimal import *
from random import uniform, randint
import os
import math
import fastavro as avro
from os import listdir
from os.path import isfile, join

MAX_NUM_ROW_BUCKET = 20
MAX_DISTANCE = 999999.0
MIN_DISTANCE = 0.0

def imprint_wo_IBPlusTree():
    # 1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='BigDataTest' user='tonhai' password='TonHai1111' host='localhost' ")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"

    # 2. Get data from files to build imprints
    #3.
    print "Finished!"


def readPTMuon(filename, output, startID):
    maxValMuon = 0
    eventIndex = 0
    bucketID = startID
    IntervalMax = MIN_DISTANCE
    IntervalMin = MAX_DISTANCE
    DBMax = MIN_DISTANCE
    DBMin = MAX_DISTANCE
    #fout = open(output, "w+")
    with open(filename, 'rb') as fo:
        reader = avro.reader(fo)
        schema = reader.schema
        counter = 0
        for record in reader:
            tempMuon = len(record['Muon'])
            print "len(record[Muon]: " + str(tempMuon)
            if (maxValMuon < tempMuon):
                maxValMuon = tempMuon
            if (tempMuon > 0):
                for j in range(0, tempMuon):
                    temppt = Decimal(record['Muon'][j]['pt'])
                    print "pt: " + str(temppt)
                #if (IntervalMax < temppt):
                #    IntervalMax = temppt
                #if (IntervalMin > temppt):
                #    IntervalMin = temppt
                #if (DBMax < temppt):
                #    DBMax = temppt
                #if (DBMin > temppt):
                    DBMin = temppt
            counter += 1
            if (counter == 10):
                break #stop
            #if (counter % MAX_NUM_ROW_BUCKET == 0):
            #    fout.write("bucketID: %d\n" % bucketID)
            #fout.write("%s\n" % ", ".join(map(str, record['Muon'])))
            #if (counter % MAX_NUM_ROW_BUCKET == 0):
            #    fout.write("interval %d: %f %f\n" % (bucketID, IntervalMin, IntervalMax))
            #    IntervalMax = MIN_DISTANCE
            #    IntervalMin = MAX_DISTANCE
            #    bucketID += 1
            #    fout.write("############################################################\n")
    #fout.close()
    #print "counter:" + str(counter)
    #print "Max # of Muon: " + str(maxValMuon)
    #print "Total number of buckets: " + str(bucketID + 1)
    #print "[" + str(DBMin) + ", " + str(DBMax) + "]"
    print "Finish!"
    return bucketID

def createImprints(filename, output, startID):
    listPT = [0.0 for x in range(0, Constants.IMPRINTS_NUM_PT)]
    with open(filename, 'rb') as fo:
        reader = avro.reader(fo)
        schema = reader.schema
        counter = 0
        print "Create imrints..."
        print "Input file: " + str(filename)
        for record in reader:
            tempMuon = len(record['Muon'])
            if (tempMuon > 0):
                for j in range(0, tempMuon):
                    temppt = Decimal(record['Muon'][j]['pt'])
                    listPT[counter] = temppt
                    counter += 1
                    if(counter == Constants.IMPRINTS_NUM_PT):
                        #print startID
                        writeImprints(listPT, output, startID)
                        counter = 0
                        startID += 1
    print "Finish!"
    return startID

def writeImprints(listPT, output, ID):
    fout = open(output, "a+")
    imprints = imprintsHash(listPT)
    fout.write("ImprintID: " + str(ID) + "\t" + str(imprints) + "\n")
    fout.close()
    return Constants.FUNC_TRUE

def createZonemaps(filename, output, startID):
    listPT = [0.0 for x in range(0, Constants.IMPRINTS_NUM_PT)]
    with open(filename, 'rb') as fo:
        reader = avro.reader(fo)
        schema = reader.schema
        counter = 0
        print "Create zonemaps..."
        print "Input file: " + str(filename)
        for record in reader:
            tempMuon = len(record['Muon'])
            if (tempMuon > 0):
                for j in range(0, tempMuon):
                    temppt = Decimal(record['Muon'][j]['pt'])
                    listPT[counter] = temppt
                    counter += 1
                    if(counter == Constants.IMPRINTS_NUM_PT):
                        writeZonemaps(listPT, output, startID)
                        counter = 0
                        startID += 1
    print "Finish!"
    return startID

def writeZonemaps(listPT, output, ID):
    fout = open(output, "a+")
    zonemaps = zonemapsHash(listPT)
    fout.write("ZoneMapID: " + str(ID) + "\t[" + str(zonemaps[0]) + ", " + str(zonemaps[1]) + "]\n")
    fout.close()
    return Constants.FUNC_TRUE

def printLen(directory):
    onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f))]
    for filename in onlyfiles:
        MaxLen = 0
        with open(str(directory) + "/" + str(filename), 'rb') as fo:
            reader = avro.reader(fo)
            schema = reader.schema
            for record in reader:
                lenMuon = len(record['Muon'])
                if(lenMuon > MaxLen):
                    MaxLen = lenMuon
        print str(filename) + ":\t" + str(MaxLen)

    return Constants.FUNC_TRUE

def initImprintsIBTree(directory, output, index):
    counter = 0
    groupIndex = 0
    #ListMuon = [[[0.0 for muon in range(0, 3000000)] for pt in range(0, 100)] for group in range(0, 100)]
    ListMuon = [[] for group in range(0, 15)]
    #ListMuon = []
    onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f))]
    print "reading..."
    numFile = 0
    for filename in onlyfiles:
        numFile += 1
        with open(str(directory) + "/" + str(filename), 'rb') as fo:
            print filename
            reader = avro.reader(fo)
            schema = reader.schema
            for record in reader:
                lenMuon = len(record['Muon'])
                if(lenMuon > 0):
                    list = []
                    for j in range(0, lenMuon):
                        tempPT = Decimal(record['Muon'][j]['pt'])
                        list.append(tempPT)
                        counter += 1
                    ListMuon[lenMuon].append(list)
        #if(numFile > 4):
        #    break
    #Sort
    print "sorting..."
    for group in range(1, 15):
        curLen = len(ListMuon[group])
        print "\t group:" + str(group) + "th!" + "\t Length: " + str(curLen)
        #for i in range(0, curLen):
        #    for j in range(i, curLen):
        #        if(max(ListMuon[group][i]) > max(ListMuon[group][j])):
        #            temp = ListMuon[group][i]
        #            ListMuon[group][i] = ListMuon[group][j]
        #            ListMuon[group][j] = temp
        if(curLen > 0):
            binarySort(ListMuon, group, 0, curLen - 1)
    #Write to file
    print "writing..."
    fout = open(output, "w+")
    for group in range(1, 15):
        for i in range(0, len(ListMuon[group])):
            fout.write(str(ListMuon[group][i]) + "\n")
    fout.close()
    return index

def getPTValue(directory, output, index):
    counter = 0
    groupIndex = 0
    fout = open(output, "w+")
    onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f))]
    print "reading & writing..."
    numFile = 0
    for filename in onlyfiles:
        numFile += 1
        with open(str(directory) + "/" + str(filename), 'rb') as fo:
            print filename
            reader = avro.reader(fo)
            schema = reader.schema
            for record in reader:
                lenMuon = len(record['Muon'])
                if(lenMuon > 0):
                    list = []
                    for j in range(0, lenMuon):
                        tempPT = Decimal(record['Muon'][j]['pt'])
                        list.append(tempPT)
                        counter += 1
                    fout.write(str(list) + "\n")
    fout.close()
    return index

def binarySort(ListMuon, group, start, end):
    if(start >= end):
        return
    if(start == (end - 1)):
        if(max(ListMuon[group][end]) < max(ListMuon[group][start])):
            temp = ListMuon[group][start]
            ListMuon[group][start] = ListMuon[group][end]
            ListMuon[group][end] = temp
        return

    mid = int((end + start)/2)
    #Move mid to top
    temp = ListMuon[group][mid]
    ListMuon[group][mid] = ListMuon[group][start]
    ListMuon[group][start] = temp

    sort = start
    for i in range(start + 1, end + 1):
        if(max(ListMuon[group][i]) < max(ListMuon[group][start])):
            sort += 1
            temp = ListMuon[group][sort]
            ListMuon[group][sort] = ListMuon[group][i]
            ListMuon[group][i] = temp
    binarySort(ListMuon, group, start, sort)
    binarySort(ListMuon, group, sort + 1, end)
    return

def sortImprintsIBTree():
    onlyfiles = [f for f in listdir('temp') if isfile(join('temp', f))]
    for filename in onlyfiles:
        with open('temp/' + str(filename), 'rb') as fo:
            print "TODO"

    return Constants.FUNC_TRUE

def test_ImprintsIBTree(initInput, initNum, input, numInput, queryInterval, startID):
    # 1. Read sorted data from InitImprintsIBTree_sorted_v3.dat file (around 1 000 000 rows = 1 000 buckets)
    listPT = [0.0 for x in range(0, Constants.IMPRINTS_NUM_PT)]
    ibTree = IBTree()
    finput = open(initInput, "r")
    counter = 0
    numRows = 0
    print("1. Read sorted data from InitImprintsIBTree_sorted_v3.dat")
    print("2 & 3. Build imprints and insert into IBTree")
    for line in finput:
        listValues = line.split('\'')
        lenList = len(listValues)
        nValue = int(lenList/2)
        for i in range(0, nValue):
            pt = float(listValues[i * 2 + 1])
            listPT[numRows] = pt
            numRows += 1
            if(numRows == Constants.IMPRINTS_NUM_PT):
                # 2. Build Imprints for these data
                interval = imprintsBucket(listPT)
                # 3. Insert these data into IBTree
                ibTree.insertBucket(interval, startID)
                numRows = 0
                startID += 1
        counter += 1
        if (counter == initNum):
            break
    finput.close()
    print("4. Copy IBTree structure to IB+-Tree")
    # 4. Copy IBTree structure to IB+Tree
    ibPlusTree = IBPlusTree(ibTree)
    ibPlusTree.copyStructure(ibTree)
    print("5. Read random data from IniImprintsIBTree_random_v4.data file")
    print("6. Insert these data into IB+-Tree")
    # 5. Read random data from InitImprintsIBTree_random_v4.data file (around 1 000 000 rows = 1 000 buckets)
    fin = open(input, "r")
    counter = 0
    for line in fin:
        listValues = line.split('\'')
        lenList = len(listValues)
        nValue = int(lenList/2)
        pts = []
        for i in range(0, nValue):
            pt = float(listValues[i * 2 + 1])
            pts.append(pt)
            #imprints = imprintsBucket(pts)
            imprintMark = imprintsHash(pts) # skip at this moment
            # 6. Insert these data into IB+-Tree and thus IB-Tree
            ibPlusTree.insertTuple(imprintMark, str(pts))
        counter += 1
        if(counter == numInput):
            break
    fin.close()
    print("7. Run the queries and obtain the results")
    # 7. Run the queries to obtain the results
    listBuckets = ListBuckets()
    listTuples = ListTuples()
    ibPlusTree.searchImprintsAll(listTuples, listBuckets, queryInterval)
    print "Result: ", listBuckets.results
    ibTree.printEvalInfo()
    ibTree.evaluation.printEvalInfoToFile("ImprintsIBTree_output.txt")
    #ibPlusTree.printEvalInfo()
    #ibPlusTree.evaluation.printEvalInfoToFile()

    return Constants.FUNC_TRUE

def imprintsBucket(listPT):
    minImprints = 0xffffffff
    maxImprints = 0x00000000
    for i in range(0, len(listPT)):
        imprints = hashValue(listPT[i])
        if(compareImprints(minImprints, imprints) > 0):
            minImprints = imprints
        if(compareImprints(maxImprints, imprints) < 0):
            maxImprints = imprints
    return [minImprints, maxImprints]

def evaluate_zonemap(input, num, interval):
    fin = open(input, 'r')
    counter = 0
    output = 0
    for line in fin:
        listValues = line.split('\t')
        temp = listValues[1][1:(len(listValues[1]) - 2)]
        vals = temp.split(',')
        valInterval = [float(vals[0]), float(vals[1])]
        if(intersect(interval, valInterval)):
            output += 1
        counter += 1
        if(counter == num):
            break # stop
    fin.close()
    return output

def evaluate_imprints(input, num, interval):
    fin = open(input, 'r')
    counter = 0
    output = 0
    for line in fin:
        listValues = line.split('\t')
        imprints = int(listValues[1])
        if (ovelapImprints(interval, imprints)):
            output += 1
        counter += 1
        if (counter == num):
            break  # stop
    fin.close()
    return output
if __name__ == '__main__':
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_0.avro',
    #                       'Imprints_100_200.txt', 0)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_1.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_2.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_3.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_4.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_5.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_6.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_7.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_8.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_9.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_10.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_11.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_12.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_13.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_14.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_15.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_16.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_17.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_18.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_19.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_20.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_21.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_22.avro',
    #                       'Imprints_100_200.txt', index)
    #index = createImprints('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_23.avro',
    #                       'Imprints_100_200.txt', index)

    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_0.avro',
    #                       'ZoneMaps_100_200.txt', 0)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_1.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_2.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_3.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_4.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_5.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_6.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_7.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_8.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_9.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_10.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_11.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_12.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_13.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_14.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_15.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_16.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_17.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_18.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_19.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_20.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_21.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_22.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #index = createZonemaps('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV/DYJetsToLL_M_50_HT_100to200_13TeV_2_23.avro',
    #                       'ZoneMaps_100_200.txt', index)
    #print index

    #printLen('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV')
    #initImprintsIBTree('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV', 'InitImprintsIBTree_random_v3.dat', 0)
    #getPTValue('../BigData/DYJetsToLL_M_50_HT_100to200_13TeV', 'InitImprintsIBTree_random_v4.dat', 0)
    test_ImprintsIBTree('InitImprintsIBTree_random_v3.dat', 1000000, 'InitImprintsIBTree_random_v4.dat', 1000000, [0x00000020, 0x00000020], 0)
    test_ImprintsIBTree('InitImprintsIBTree_random_v3.dat', 1000000, 'InitImprintsIBTree_random_v4.dat', 1000000,
                        [0x00000010, 0x00000010], 0)
    test_ImprintsIBTree('InitImprintsIBTree_random_v3.dat', 1000000, 'InitImprintsIBTree_random_v4.dat', 1000000,
                        [0x00000008, 0x00000008], 0)
    test_ImprintsIBTree('InitImprintsIBTree_random_v3.dat', 1000000, 'InitImprintsIBTree_random_v4.dat', 1000000,
                        [0x00000004, 0x00000004], 0)
    print "-----------------------\n"
    number = 0
    number = evaluate_zonemap('ZoneMaps_100_200.txt', 2000, [3.0, 3.5])
    print number
    print "-----------------------\n"
    number = evaluate_imprints('Imprints_100_200.txt', 2000, 0x0000003d)
    print number
    print "-----------------------\n"