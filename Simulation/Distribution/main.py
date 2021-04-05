#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 13:53:29 2019

@author: tanumalik
"""
import distribution
import sort
import window
import interval
import pickle as pickle
import copy
import math

samples_bkp = copy.deepcopy(distribution.samples)
samples_bkp.sort()
#print(samples_bkp)

    
total_disk_io = 0
for i in xrange(0,distribution.num_queries-1,1):
    range_a = distribution.query[i]
    range_b = distribution.query[i+1]
    cnt = 0
    bucket_intersect_on_disk = 0
    for i in xrange(0,len(samples_bkp)):
        if (samples_bkp[i] >= range_a) and (samples_bkp[i] <= range_b):
            cnt = cnt+1
    
    if (cnt > 0):
        bucket_intersect_on_disk = math.ceil(float(cnt)/interval.window_size)
#    start = 0
#    end = 0;
#    bucket_intersect_on_disk = 0
#    bucket_intersect_in_mem = 0
#    start = binarysearch(samples_bkp,0,len(samples_bkp),range_a)
#    end = binarysearch(samples_bkp,0,len(samples_bkp),range_b)
#    if (start > 0):
#        if (end > 0):
#            if (start == end):
#                bucket_intersect_on_disk  = 1 
#            else: 
#                bucket_intersect_on_disk  = end-start   
#    else:
#        if (end >0):
#            bucket_intersect_on_disk  = 1 
    #print(range_a,range_b,bucket_intersect_on_disk)
    total_disk_io = total_disk_io + bucket_intersect_on_disk   
print("Total Disk IO" + str(total_disk_io))



#execfile("/Users/tanumalik/Dropbox/Work/ResearchProjects/Meshede/HPC-DB/PLI+/distribution.py")
#execfile("/Users/tanumalik/Dropbox/Work/ResearchProjects/Meshede/HPC-DB/PLI+/sort.py")

total_disk_io = 0

bucket_intervals_on_disk = []
with open('sort-disk.txt', "rb") as f:
    for _ in range(sort.num_intervals_on_disk):
       l = pickle.load(f)
       #print(l)
       bucket_intervals_on_disk.append(l)

#print(bucket_intervals_on_disk)

bucket_intervals_in_mem = []
with open('sort-mem.txt', "rb") as f:
    for _ in range(sort.num_intervals_in_mem):
        bucket_intervals_in_mem.append(pickle.load(f))

for i in xrange(0,distribution.num_queries-1,1):
    range_a = distribution.query[i]
    range_b = distribution.query[i+1]
    bucket_intersect_on_disk = 0
    bucket_intersect_in_mem = 0
    #print(sort.num_intervals_on_disk)
    #print(range_a,range_b)
    for j in xrange(0,sort.num_intervals_on_disk):      
        if ((range_a >= bucket_intervals_on_disk[j][2]) and (range_a <= bucket_intervals_on_disk[j][3])) or ((range_b >= bucket_intervals_on_disk[j][2]) and (range_b <= bucket_intervals_on_disk[j][3])):
                #print(j,bucket_intervals_on_disk[j][2],bucket_intervals_on_disk[j][3])
                bucket_intersect_on_disk = bucket_intersect_on_disk + 1;
                #print(bucket_intersect_on_disk)
    total_disk_io = total_disk_io + bucket_intersect_on_disk            
#     for j in xrange(0,num_intervals_in_mem):
#         if ((range_a >= bucket_intervals_in_mem[j][2]) and (range_a <= bucket_intervals_in_mem[j][3])) or ((range_b >= bucket_intervals_in_mem[j][2]) and (range_b <= bucket_intervals_in_mem[j][3])):
#                 bucket_intersect_in_mem = bucket_intersect_in_mem + 1;
#     #print(str(i) + " " + "Q_A: " + str(range_a) + "Q_B "+ str(range_b) + "Num_disk: " + str(bucket_intersect_on_disk) + "Num_mem: " + str(bucket_intersect_in_mem))
print("Total Disk IO" + str(total_disk_io))    

#execfile("/Users/tanumalik/Dropbox/Work/ResearchProjects/Meshede/HPC-DB/PLI+/window.py")

total_disk_io = 0

bucket_intervals_on_disk = []
with open('window-disk.txt', "rb") as f:
    for _ in range(len(window.window_intervals)):
        l = pickle.load(f)
        #print(l)
        bucket_intervals_on_disk.append(l)
#print(bucket_intervals_on_disk)

bucket_intervals_in_mem = []
with open('window-mem.txt', "rb") as f:
    for _ in range(window.num_intervals_in_mem):
        bucket_intervals_in_mem.append(pickle.load(f))


for i in xrange(0,distribution.num_queries-1,1):
    range_a = distribution.query[i]
    range_b = distribution.query[i+1]
    bucket_intersect_on_disk = 0
    bucket_intersect_in_mem = 0
    #print('query',range_a,range_b)
    for j in xrange(0,len(window.window_intervals)):
        #if ((range_a >= bucket_intervals_on_disk[j][2]) and (range_a <= bucket_intervals_on_disk[j][3])) or ((range_b >= bucket_intervals_on_disk[j][2]) and (range_b <= bucket_intervals_on_disk[j][3])):
        #        bucket_intersect_on_disk = bucket_intervals_on_disk[j][4];
        if ((range_a >= bucket_intervals_on_disk[j][0]) and (range_a <= bucket_intervals_on_disk[j][1])) or ((range_b >= bucket_intervals_on_disk[j][0]) and (range_b <= bucket_intervals_on_disk[j][1])):
                bucket_intersect_on_disk = bucket_intersect_on_disk + 1
                #print(j,bucket_intervals_on_disk[j][2],bucket_intervals_on_disk[j][3],bucket_intersect_on_disk)
    total_disk_io = total_disk_io + bucket_intersect_on_disk
#     for j in xrange(0,num_intervals_in_mem):
#         if ((range_a >= bucket_intervals_in_mem[j][2]) and (range_a <= bucket_intervals_in_mem[j][3])) or ((range_b >= bucket_intervals_in_mem[j][2]) and (range_b <= bucket_intervals_in_mem[j][3])):
#                 bucket_intersect_in_mem = bucket_intersect_in_mem + 1;
#     #print(str(i) + " " + "Q_A: " + str(range_a) + "Q_B "+ str(range_b) + "Num_disk: " + str(bucket_intersect_on_disk) + "Num_mem: " + str(bucket_intersect_in_mem))
print("Total Disk IO" + str(total_disk_io))    
#execfile("/Users/tanumalik/Dropbox/Work/ResearchProjects/Meshede/HPC-DB/PLI+/interval.py")

total_disk_io = 0

bucket_intervals_on_disk = []
with open('interval-disk.txt', "rb") as f:
    for _ in range(interval.num_intervals_on_disk):
        l = pickle.load(f)
        bucket_intervals_on_disk.append(l)

bucket_intervals_in_mem = []
with open('interval-mem.txt', "rb") as f:
    for _ in range(interval.num_intervals_in_mem):
        bucket_intervals_in_mem.append(pickle.load(f))

for i in xrange(0,distribution.num_queries-1,1):
    range_a = distribution.query[i]
    range_b = distribution.query[i+1]
    
    cnt = 0
    bucket_intersect_on_disk_1 = 0
    for i in xrange(0,len(samples_bkp)):
        if (samples_bkp[i] >= range_a) and (samples_bkp[i] <= range_b):
            cnt = cnt+1
    if (cnt > 0):
        bucket_intersect_on_disk_1 = math.ceil(float(cnt)/interval.window_size)
    
    
    bucket_intersect_on_disk = 0
    bucket_intersect_in_mem = 0
    #print(interval.num_intervals_on_disk)
    for j in xrange(0,interval.num_intervals_on_disk):
        if ((range_a >= bucket_intervals_on_disk[j][2]) and (range_a <= bucket_intervals_on_disk[j][3])) or ((range_b >= bucket_intervals_on_disk[j][2]) and (range_b <= bucket_intervals_on_disk[j][3])):
                bucket_intersect_on_disk = bucket_intersect_on_disk + 1;
    #print(range_a,range_b,bucket_intersect_on_disk_1,bucket_intersect_on_disk)
    total_disk_io = total_disk_io + bucket_intersect_on_disk
#     for j in xrange(0,num_intervals_in_mem):
#         if ((range_a >= bucket_intervals_in_mem[j][2]) and (range_a <= bucket_intervals_in_mem[j][3])) or ((range_b >= bucket_intervals_in_mem[j][2]) and (range_b <= bucket_intervals_in_mem[j][3])):
#                 bucket_intersect_in_mem = bucket_intersect_in_mem + 1;
#     #print(str(i) + " " + "Q_A: " + str(range_a) + "Q_B "+ str(range_b) + "Num_disk: " + str(bucket_intersect_on_disk) + "Num_mem: " + str(bucket_intersect_in_mem))
print("Total Disk IO" + str(total_disk_io))  

