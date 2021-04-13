import pickle
from DataDistribution import *
####################################Test Functions######################################
#samples1=random_custDist(x0,x1,custDist=custDist1,size=sample_size)
#samples2=random_custDist(x0,x1,custDist=custDist2,size=sample_size)
#samples1.sort()
#samples2.sort()
#print(samples1)
#print(samples2)

############################## Initialize Range and Sequence Length ############################################
x0=0
x1=100
sample_size = 148000000

############################## Initialize Buffer Cache ############################################
tuple_size = 500   #50 bytes
buffer_size = 200000 # =(100 * 1000 * 1000)/tuple_size

######TM: initialize the buffer with -1 assuming no negative numbers
def zerolistmaker(n):
    listofzeros = [-1] * n
    return listofzeros
buffer_cache = zerolistmaker(buffer_size)

############################## Initialize Window Intervals with Buffer cache Indexes ############################################

window_size = int((1 * 1000 * 1000)/int(tuple_size))
h,w = buffer_size-window_size+1, 5
window_intervals = [[0 for x in range(w)] for y in range(h)]
#print(window_intervals)

from itertools import islice
def window(window_intervals, seq, n=2):
    print(n)
    "Returns a sliding window (of width n) over data from the iterable"
    "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        window_intervals[result[0]][0] = result[0]
        window_intervals[result[0]][1] = result[window_size-1]
        #print(result)
    for elem in it:
        result = result[1:] + (elem,)
        window_intervals[result[0]][0] = result[0]
        window_intervals[result[0]][1] = result[window_size-1]
        #print(result)

buffer_index= range(0,buffer_size)
#print(buffer_index)
window(window_intervals,buffer_index,window_size)
#print(window_intervals)

############################ Intilialize Bucket Intervals currently as a list###################################
#w1 = 5; [0] min window idx [1] max window idx [2] min value of window [3] max value of window [4] max value-min value
# Note: max and min values do not correstpond to window idx
#h1,w1 = sample_size-window_size+1, 5;
#bucket_intervals_on_disk = [[0 for x in range(w1)] for y in range(h1)]
#bucket_intervals_in_mem = [[0 for x in range(w1)] for y in range(h1)]
#outF1 = open("bucket_intervals_on_disk.txt", "a")
#outF2 = open("bucket_intervals_in_mem.txt", "a")
num_intervals_on_disk = 0
num_intervals_in_mem = 0
print("Done")

############################## functions for Cache Fill and Evict ############################################
########## For each window interval, determine min and max from buffer_cache and compute max-min
def compute_min_max():
    for x in window_intervals:
        min = x1+1
        max= -1
        for i in xrange(x[0],x[1]+1):
            if (min > buffer_cache[i]):
                min = buffer_cache[i]
            if (max < buffer_cache[i]):
                max = buffer_cache[i]
        x[2] = min
        x[3] = max
        x[4] = abs(max - min)
        #print(x)
    return

######### Generate sample distribution and fill in buffer where buffer cache is empty
def fill_in_buffer(numfill):
    #print("Numfill " +str(numfill))
    #samples=random_custDist(x0,x1,custDist=custDist1,size=numfill)
    samples=random_custDist(x0,x1,custDist=custDist2,size=numfill)
    #sort the samples
    #samples.sort()
    #print("Samples")
    #print(samples)
    j = 0
    for i in xrange(0,len(buffer_cache)):
        if (buffer_cache[i] == -1):
            #print(i)
            buffer_cache[i] = samples[j]
            j += 1
    buffer_cache.sort()
    compute_min_max()
    if (j != numfill):
        print("Gross Error")
        exit()
    return j


#def compute_eviction_decision():
#     for i in xrange(0,len(window_intervals)):
#          (window_intervals[i])[2] =  abs(buffer_cache[(window_intervals[i])[0]] - buffer_cache[(window_intervals[i])[1]])
#eviction_decision()
#print(window_intervals)

##################Find min compact value; Find window idx of min compact value; Save bucket on disk; Set
##################elements of window to -1
def evict(num_intervals_on_disk):
    num_evicted = 0
    min_compactness_value = min(x[4] for x in window_intervals)
    #print(min_compactness_value)
    for x in window_intervals:
        if (x[4] == min_compactness_value):
            idx_evict_bucket = x[0]
            #print("Evicting" + str(idx_evict_bucket))
            #copy the evicted bucket to disk
            #bucket_intervals_on_disk[num_intervals_on_disk][0] = x[0]
            #bucket_intervals_on_disk[num_intervals_on_disk][1] = x[1]
            #bucket_intervals_on_disk[num_intervals_on_disk][2] = x[2]
            #bucket_intervals_on_disk[num_intervals_on_disk][3] = x[3]
            #bucket_intervals_on_disk[num_intervals_on_disk][4] = x[4]
            with open('bucket_intervals_on_disk.txt', 'a') as filehandle:
                    # store the data as binary data stream
                    pickle.dump(x, filehandle)
            num_intervals_on_disk = num_intervals_on_disk + 1
            #evict
            for i in xrange(window_intervals[idx_evict_bucket][0],(window_intervals[idx_evict_bucket][1]+1)):
                buffer_cache[i] = -1
                num_evicted = num_evicted +1
    #print(buffer_cache)
    return num_evicted,num_intervals_on_disk
#fill_in_buffer(num_fill)


#################################### Main Algorithm ##################################################
num_fill = to_fill = buffer_size
while num_fill <= sample_size:
    full = fill_in_buffer(to_fill)
    #print(full)
    if (full > 0):
        #compute_eviction_decision()
        to_fill,num_intervals_on_disk = evict(num_intervals_on_disk)
        num_fill = num_fill + to_fill
        print("NF:" + str(num_fill) + "TF:" + str(to_fill))
    elif (full <= 0):
            exit

print("Done!")
print(bucket_intervals_on_disk)
for x in window_intervals:
    #bucket_intervals_in_mem[num_intervals_in_mem][0] = x[0]
    #bucket_intervals_in_mem[num_intervals_in_mem][1] = x[1]
    #bucket_intervals_in_mem[num_intervals_in_mem][2] = x[2]
    #bucket_intervals_in_mem[num_intervals_in_mem][3] = x[3]
    #bucket_intervals_in_mem[num_intervals_in_mem][4] = x[4]
    with open('bucket_intervals_in_mem.txt', 'wb') as filehandle:
            # store the data as binary data stream
            pickle.dump(x, filehandle)
    num_intervals_in_mem = num_intervals_in_mem +1

print("In mem: " + str(num_intervals_in_mem) + " On disk:" + str(num_intervals_on_disk))
print(buffer_cache)

  ##############   Query the Distribution ######################################
import random
a= x0
b = x1
num_queries = 1000000
x = a
query = []
for i in range(num_queries, 0, -1):
    x += (b-x) * (1 - pow(random.random(), 1. / i))
    query.append(x)
#print(query)

#total_disk_io = 0
#for i in xrange(0,num_queries-1,1):
#    range_a = query[i]
#    range_b = query[i+1]
#    bucket_intersect_on_disk = 0
#    bucket_intersect_in_mem = 0
#    for j in xrange(0,num_intervals_on_disk):
#        if ((range_a >= bucket_intervals_on_disk[j][2]) and (range_a <= bucket_intervals_on_disk[j][3])) or ((range_b >= bucket_intervals_on_disk[j][2]) and (range_b <= bucket_intervals_on_disk[j][3])):
#                bucket_intersect_on_disk = bucket_intersect_on_disk + 1;
#                total_disk_io = total_disk_io + bucket_intersect_on_disk
#    for j in xrange(0,num_intervals_in_mem):
#        if ((range_a >= bucket_intervals_in_mem[j][2]) and (range_a <= bucket_intervals_in_mem[j][3])) or ((range_b >= bucket_intervals_in_mem[j][2]) and (range_b <= bucket_intervals_in_mem[j][3])):
#                bucket_intersect_in_mem = bucket_intersect_in_mem + 1;
    #print(str(i) + " " + "Q_A: " + str(range_a) + "Q_B "+ str(range_b) + "Num_disk: " + str(bucket_intersect_on_disk) + "Num_mem: " + str(bucket_intersect_in_mem))
#print("Total Disk IO" + str(total_disk_io))

