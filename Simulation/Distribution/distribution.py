#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 14:01:38 2019

@author: tanumalik
"""
import numpy as np
import random
import pickle
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

############################## Initialize Range and Sequence Length ############################################
x0=0
x1=100
sample_size = 10000
samples=random_custDist(x0,x1,custDist=custDist1,size=sample_size)
#filehandle = open('samples1.csv', 'r')
#samples = pickle.load(filehandle)
sample_size = len(samples)
#samples_ctr = 0
#print(samples)
print(sample_size)






##############   Query the Distribution ######################################
a= x0
b = x1
num_queries = 1000#0#00
x = a
query = []
for i in range(num_queries, 0, -1):
    x += (b-x) * (1 - pow(random.random(), 1. / i))
    query.append(x)
#print(query)