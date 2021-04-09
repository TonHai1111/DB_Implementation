import numpy as np
import pickle
import random

############################## Helper Functions for Distribution ############################################
def custDist1(x):
    if ((x > 0) & (x < 50)):
        return 0.05
    elif ((x >= 50) & (x <= 55)):
         return 0.9
    elif ((x > 55) & (x <= 100)):
         return 0.05
        #return (np.exp(x-2008)-1)/(np.exp(2019-2007)-1)

def custDist2(x):
    if ((x>0) & (x<50)):
        return 0.45
    elif ((x >= 50) & (x <= 55)):
         return 0.1
    elif ((x > 55) & (x <= 100)):
         return 0.45

########### Data distribution generators ################
'''
Uniform_generator:
Input: min, max, size, mode
Output: output = {values} //an array of size random values in the range of [min, max]
       mode = 0: integer
       mode = 1: float
The propability density of function of the uniform generator:
 p(x) = 1/(max - min)  , for x in [min, max)
 p(x) = 0              , for x elsewhere
'''
def Uniform_generator(min=0, max=10000, size=1000, mode=0):
    if (mode == 0):
        result = np.random.randint(min, max, size) #randint is also a uniform distribution
        return result
    else: 
        result = np.random.uniform(min, max, size)
        return result

'''
Gaussian_generator or Normal_generator
Input: size, mode, mean, variance
Output: output = {values}  //an array of size values 
                           //value distribution goes with gaussian distribution or normal distribution
                           // with given mean and variance
       mode = 0: integer
       mode = 1: float
f(x) = [1/sqrt(2.pi.variance)].[e^{-[(x-mean)^2/(2.variance)]}]
'''
def Gaussian_generator(size=1000, mode=0, mean=5000.0, variance=1.0):
    result = []
    if (mode == 0):
        temp = np.rint(float(mean) + float(variance) * np.random.randn(size))
        result = temp.astype(int)
        return result
    else: #mode ==1
        result = float(mean) + float(variance) * np.random.randn(size)
        return result

'''
Triangular_generator
Input: min, max, size, mode, peak
Output: output = {values}  //an array of size values that belongs to [min, max]
                           //value distribution goes with triangular distribution
                           // with given min, max and peak
       mode = 0: integer
       mode = 1: float
p(x) = 2(x-l)/[(max-min)(peak-min)]     , for x in [min, peak]
p(x) = 2(max-x)/[(max-min)(peak-min)]   , for x in [peak, max]
p(x) = 0                                , for x elsewhere
'''
def Triangular_generator(min=0, max=10000, size=1000, mode=0, peak=5000.0):
    result = []
    if (mode == 0):
        temp = np.rint(np.random.triangular(min, peak, max, size))
        result = temp.astype(int)
        return result
    else: #mode ==1
        result = np.random.triangular(min, peak, max, size)
        return result

'''
Wald_generator (or inverse Gaussian)
Input: size, mode, mean, scale
Output: output = {values}  //an array of size values that belongs to [min, max]
                           //value distribution goes with Wald distribution or inverse Gaussian distribution
                           // with given mean and scale
       mode = 0: integer
       mode = 1: float
p(x) = sqrt[scale/(2.pi.x^3)].e^{[-scale.(x-mean)^2]/(2.x.mean^2)}
'''
def Wald_generator(size=1000, mode=0, mean=5000.0, scale=1):
    result = []
    if (mode == 0):
        temp = np.rint(np.random.triangular(mean, scale, size))
        result = temp.astype(int)
        return result
    else: #mode ==1
        result = np.random.wald(mean, scale, size)
        return result


def random_custDist(x0,x1,custDist,size=None, nControl=10**6):
    #genearte a list of size random samples, obeying the distribution custDist
    #suggests random samples between x0 and x1 and accepts the suggestion with probability custDist(x)
    #custDist noes not need to be normalized. Add this condition to increase performance.
    #Best performance for max_{x in [x0,x1]} custDist(x) = 1
    samples=[]
    nLoop=0
    while len(samples)<size and nLoop<nControl:
        x=np.random.uniform(low=x0,high=x1)
        prop=custDist(x)
        assert prop>=0 and prop<=1
        if np.random.uniform(low=0,high=1) <=prop:
            samples += [x]
        nLoop+=1
    return samples
####################################Test Functions######################################
#samples1=random_custDist(x0,x1,custDist=custDist1,size=sample_size)
#samples2=random_custDist(x0,x1,custDist=custDist2,size=sample_size)
#samples1.sort()
#samples2.sort()
#print(samples1)
#print(samples2)
'''
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

'''