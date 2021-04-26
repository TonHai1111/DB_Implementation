from os import listdir
from os.path import isfile, join
from os import walk
import subprocess
import csv
import ntpath
import os
from BTree import *

def get_all_full_filenames(path):
    files = []
    for item in listdir(path):
        item_fullpath = join(path, item)
        if isfile(item_fullpath):
            files.append(item_fullpath)
        else:
            f = get_all_full_filenames(item_fullpath)
            files.extend(f)
    return files

def get_all_files(path):
    f = []
    for (dirpath, dirnames, filenames) in walk(path):
        f.extend(filenames)
    return f

def main():
    list_paths = ["./NYC/nyc-taxi-data-master/data"]
    filenames = []
    for mypath in list_paths:
        f = get_all_full_filenames(mypath)
        filenames.extend(f)
    '''
    treedata = open("cvs_treedata_fields.log", "w")
    plotdata = open("cvs_plotdata_fields.log", "w")
    for f in filenames:
        print(f)
        name = os.path.basename(f)
        if name.lower().startswith('gedicalval_treedata'):
            fread = open(f, 'r')
            fline = fread.readline()
            treedata.write(name)
            treedata.write('\n')
            treedata.write(fline)
            fread.close()
        if name.lower().startswith('gedicalval_plotdata'):
            fread = open(f, 'r')
            fline = fread.readline()
            plotdata.write(name)
            plotdata.write('\n')
            plotdata.write(fline)
            fread.close()
    treedata.close()
    plotdata.close()
    '''
    b = BTree(15)
    count = 0
    dic = {}
    for f in filenames:
        print f
        name = os.path.basename(f)
        if (name.lower().startswith('yellow')):
            print ("\t" + name)
            fread = open(f, 'r')
            header = fread.readline().split(',')
            fread.readline()
            for line in fread:
                values = line.split(',')
                if(not b._present(values[4], b._path_to(values[4]))):
                    b.insert(values[4])
                    count += 1
                    dic[values[4]] = 1
                else:
                    dic[values[4]] += 1
            fread.close()
        elif(name.lower().startswith('green')):
            print ("\t" + name)
            fread = open(f, 'r')
            header = fread.readline().split(',')
            fread.readline()
            for line in fread:
                values = line.split(',')
                if(not b._present(values[10], b._path_to(values[10]))):
                    b.insert(values[10])
                    count += 1
                    dic[values[10]] = 1
                else:
                    dic[values[10]] += 1
            fread.close()
        print count
    print ("total number of values: " + str(count))
    fout = open("output.txt", "w")
    for item in dic.items():
        fout.write(str(item) + '\n')
    fout.close()
            
if __name__ == "__main__":
    
    main()       
    print("Done!")