#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 00:08:59 2019

@author: tanumalik
"""

import pickle
import distribution

######TM: initialize the buffer with -1 assuming no negative numbers
def zerolistmaker(n):
    listofzeros = [-1] * n
    return listofzeros

def compute_min_max():
    for x in window_intervals:
        min = distribution.x1+1
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

def evict_window(filehandle,num_intervals_on_disk):
    num_evicted = 0
    min_compactness_value = min(x[4] for x in window_intervals)
    #print(min_compactness_value)
    for x in window_intervals:
        if (x[4] == min_compactness_value):
            idx_evict_bucket = x[0]
            pickle.dump(x, filehandle)
            num_intervals_on_disk = num_intervals_on_disk + 1
            break
    for i in xrange(window_intervals[idx_evict_bucket][0],(window_intervals[idx_evict_bucket][1]+1)):
        buffer_cache[i] = -1
        num_evicted +=1
    return idx_evict_bucket,num_evicted,num_intervals_on_disk
#fill_in_buffer(num_fill)

def fill_in_buffer(window_idx,numfill,samples_ctr):
    if (window_idx == -1) and (numfill == buffer_size):
        for i in xrange(0,buffer_size):
            if (buffer_cache[i] == -1):
                buffer_cache[i] = distribution.samples[samples_ctr]
                samples_ctr +=1
    elif (numfill == window_size):
        for i in xrange(window_intervals[window_idx][0], window_intervals[window_idx][1]+1):
            #print(i,samples_ctr)
            if (samples_ctr < distribution.sample_size):
                buffer_cache[i] = distribution.samples[samples_ctr]
                samples_ctr +=1
            elif (samples_ctr == distribution.sample_size):
                return -1
    buffer_cache.sort()
    compute_min_max()
    return samples_ctr   

#execfile("/Users/tanumalik/Dropbox/Work/ResearchProjects/Meshede/HPC-DB/PLI+/distribution.py")
filehandle = open('interval-disk.txt', 'wb')
evict_policy = 'interval'

samples_ctr = 0

buffer_size = 1000
buffer_cache = zerolistmaker(buffer_size)

window_size = 10
h,w = buffer_size-window_size+1, 5;
window_intervals = [[0 for x in range(w)] for y in range(h)] 
#print(window_intervals)

from itertools import islice

def window(window_intervals, seq, n=2):
    "Returns a sliding window (of width n) over data from the iterable"
    "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        #print(result[0])
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

num_intervals_on_disk = 0
num_intervals_in_mem = 0
print("Done")

num_fill = 0
to_fill = buffer_size
window_idx = -1
while (samples_ctr) < distribution.sample_size:
    samples_ctr = fill_in_buffer(window_idx,to_fill,samples_ctr) 
    if (samples_ctr > 0):
        window_idx, to_fill, num_intervals_on_disk = evict_window(filehandle,num_intervals_on_disk)


filehandle.close()
print("Done Intervals!")
#print(bucket_intervals_on_disk)
filehandle = open('interval-mem.txt', 'wb')
for x in window_intervals:
    # store the data as binary data stream
    pickle.dump(x, filehandle)
    #bucket_intervals_in_mem[num_intervals_in_mem][0] = x[0]
    #bucket_intervals_in_mem[num_intervals_in_mem][1] = x[1]
    #bucket_intervals_in_mem[num_intervals_in_mem][2] = x[2]
    #bucket_intervals_in_mem[num_intervals_in_mem][3] = x[3]
    #bucket_intervals_in_mem[num_intervals_in_mem][4] = x[4]
    num_intervals_in_mem = num_intervals_in_mem +1
filehandle.close()    
print("In mem: " + str(num_intervals_in_mem) + " On disk:" + str(num_intervals_on_disk))
#print(buffer_cache)