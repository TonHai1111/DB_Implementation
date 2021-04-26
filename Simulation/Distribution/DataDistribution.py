import numpy as np
import random

########### Data distribution generators ################
'''
#TODO: Remove the following original source code of custDist1, custDist2, 
#   since they were rewritten using custDist function

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
'''

'''
custDist:
input:  - interval: list of intervals: 
                { (interval[0], interval[1]], 
                (interval[1], interval[2]], 
                ... 
                (interval[n-1], interval[n]] }
        - percentage: list of percentage for those intervals (total should be 1)
                {percentage[0],
                percentage[1],
                ...
                percentage[n-1]} 
        - x: the input value
Output: the percentage of the interval that x belongs to.

For example custDist1 and custDist2 can be rewritten as follows: 
    - custDist1 (x) = custDist([0, 50, 55, 100], [0.05, 0.9, 0.05], x)
    - custDist2 (x) = custDist([0, 50, 55, 100], [0.45, 0.1, 0.45], x)
'''
def custDist(interval, percentage, x):
    for i in range(len(interval)):
        if(x > interval[i] & x <= interval[i+1]):
            return percentage[i]
    return -1

def custDist1(x):
    return custDist([0, 50, 55, 100], [0.05, 0.9, 0.05], x)

def custDist2(x):
    return custDist([0, 50, 55, 100], [0.45, 0.1, 0.45], x)

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
        #np.random.randn()
        #if np.random.uniform(low=0,high=1) <=prop:
        #    samples += [x]
        samples += [np.random.uniform(low=0, high=prop)]
        nLoop+=1
    return samples