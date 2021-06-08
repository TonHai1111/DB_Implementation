from os import listdir
from os.path import isfile, join
from os import walk
import subprocess
import csv
import ntpath
import os

MAX_RANGE = 9999999999999

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

# Find position of sorting array
def binaryFind_pos(ListValue, start, end, value):
    if(start > end): #ListValue is empty
        return 0
    if(start == end):
        if(float(value) < float(ListValue[start])):
            return start
        else:
            return start + 1

    mid = int((end + start)/2)
    if(float(value) < float(ListValue[mid])):
        return binaryFind_pos(ListValue, start, mid, value)
    else:
        return binaryFind_pos(ListValue, mid + 1, end, value)

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

def evict_bucket(cache, cache_count, cache_limit, bucket_size, f_output_MinMax):
    if(cache_count < bucket_size or cache_count > cache_limit):
        print ("Input ERROR: cache count is smaller than bucket size or larger than cache limit!")
        return
    # Find bucket
    i = 0
    end = cache_count
    MinMax = MAX_RANGE
    selected = 0
    while (i < end - bucket_size + 1):
        cur_MinMax = float(cache[i + bucket_size - 1]) - float(cache[i])
        if(MinMax > cur_MinMax):
            MinMax = cur_MinMax
            selected = i
        i += 1
    # Write bucket info
    write_str = str(cache[selected]) + ", " + str(cache[selected + bucket_size - 1]) + "\n"
    f_output_MinMax.write(write_str)
    # Evict bucket from cache
    i = 0
    while (i < bucket_size):
        cache.pop(selected)
        i += 1
    return

def main():
    list_paths = ["./NYC/nyc-taxi-data-master/data"]
    filenames = []
    for mypath in list_paths:
        f = get_all_full_filenames(mypath)
        filenames.extend(f)
    
    cache = []
    cache_limit = 127875 #  80 * 1024 * 1024 / 656
    bucket_size = 800 # 512 * 1024 / 656
    output_MinMax_file = "Min_Max_Buckets.txt"
    f_output_MinMax = open(output_MinMax_file, 'w')
    cache_count = 0
    count = 0
    for f in filenames:
        print (f)
        name = os.path.basename(f)
        if (name.lower().startswith('yellow')):
            print ("\t" + name)
            fread = open(f, 'r')
            header = fread.readline().split(',')
            fread.readline()
            for line in fread:
                values = line.split(',')
                count += 1
                #insert values[4] to cache[]
                #i = 0
                #while (i < cache_count):
                #    if(cache[i] > values[4]):
                #        break
                #    i += 1
                i = binaryFind_pos(cache, 0, cache_count - 1, values[4])

                cache.insert(i, values[4])
                cache_count += 1
                if (cache_count == cache_limit): #Evict 1 bucket
                    evict_bucket(cache, cache_count, cache_limit, bucket_size, f_output_MinMax)
                    cache_count = cache_count - bucket_size
            fread.close()
        elif(name.lower().startswith('green')):
            print ("\t" + name)
            fread = open(f, 'r')
            header = fread.readline().split(',')
            fread.readline()
            for line in fread:
                values = line.split(',')
                count += 1
                #insert values[10] to cache[]
                #i = 0
                #while (i < cache_count):
                #    if(cache[i] > values[10]):
                #        break
                #    i += 1
                i = binaryFind_pos(cache, 0, cache_count - 1, values[10])
                cache.insert(i, values[10])
                cache_count += 1
                if (cache_count == cache_limit): #Evict 1 bucket
                    evict_bucket(cache, cache_count, cache_limit, bucket_size, f_output_MinMax)
                    cache_count = cache_count - bucket_size
            fread.close()
        print (count)
    f_output_MinMax.close()
    print ("total number of values: " + str(count))

def count_IO(MinMax_file, query_range):
    result = 0
    file_Min_Max = open(MinMax_file, 'r')
    for line in file_Min_Max:
        values = line.split(', ')
        if((float(query_range[0]) < float(values[0]))):
            if(float(query_range[1]) >= float(values[0])):
                result += 1
        elif (float(query_range[0]) == float(values[0])):
            result += 1
        else:
            if(float(query_range[0]) <= float(values[1])):
                result += 1
    file_Min_Max.close()
    return result

if __name__ == "__main__":
    
    #main()
    count = count_IO("Min_Max_Buckets_2.txt", [3.9, 5]);       
    print (count)
    print("Done!")