#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 14:00:13 2019

@author: tanumalik
"""
import pickle
import distribution

######TM: initialize the buffer with -1 assuming no negative numbers
def zerolistmaker(n):
    listofzeros = [-1] * n
    return listofzeros

def evict_sort(filehandle,num_intervals_on_disk):
    num_evicted = 0
    window_start = 0
    window_end = 0
    
    for i in xrange(0,buffer_size/window_size): # go over each window. 0,100
        x = []
        window_start = i*window_size
        #window_start = i*buffer_size/window_size   # idx of start window
        window_end = (i+1)*window_size -1
        #window_end = (i+1)*buffer_size/window_size-1 #idx of end eindow
        #print(window_start, window_end)
        #print(window_start,window_end)
        x.append(window_start)
        x.append(window_end)
        x.append(buffer_cache[window_start])
        x.append(buffer_cache[window_end])
        #print(x)
        pickle.dump(x, filehandle)
        num_intervals_on_disk = num_intervals_on_disk + 1
        for i in xrange(window_start,window_end+1):
            buffer_cache[i] = -1
            num_evicted = num_evicted + 1
        #print(x)
    return num_evicted,num_intervals_on_disk

def fill_in_buffer(samples_ctr):
    for i in xrange(0,buffer_size):
        if (buffer_cache[i] == -1):
           buffer_cache[i] = distribution.samples[samples_ctr]
           samples_ctr += 1
    buffer_cache.sort()
    #print(buffer_cache)    
    return samples_ctr



#################################### Main Algorithm ##################################################
#execfile("/Users/tanumalik/Dropbox/Work/ResearchProjects/Meshede/HPC-DB/PLI+/distribution.py")
filehandle = open('sort-disk.txt', 'wb')
evict_policy = 'sort'
samples_ctr = 0
buffer_size = 1000
buffer_cache = zerolistmaker(buffer_size)
window_size = 10
num_intervals_on_disk = 0
num_intervals_in_mem = 0

print(distribution.sample_size)
while (samples_ctr+buffer_size) <= distribution.sample_size:
    samples_ctr = fill_in_buffer(samples_ctr) 
    print("Filled:" + str(samples_ctr))
    if (samples_ctr > 0):
        to_fill,num_intervals_on_disk = evict_sort(filehandle,num_intervals_on_disk)      
    else:
            exit

filehandle.close()
print("Done!")
#print(bucket_intervals_on_disk)

filehandle = open('sort-mem.txt', 'wb')

for i in xrange(0,buffer_size/window_size):
    x = []
    window_start = i*window_size
    #window_start = i*buffer_size/window_size   # idx of start window
    window_end = (i+1)*window_size -1
        
    #window_start = i*buffer_size/window_size
    #window_end = (i+1)*buffer_size/window_size-1
    #print(window_start,window_end)
    x.append(window_start)
    x.append(window_end)
    x.append(buffer_cache[window_start])
    x.append(buffer_cache[window_end])
    pickle.dump(x, filehandle)
    num_intervals_in_mem += 1
filehandle.close()    


print("In mem: " + str(num_intervals_in_mem) + " On disk:" + str(num_intervals_on_disk))
#print(buffer_cache)