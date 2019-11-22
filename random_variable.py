import sys
import os
import random
import math
import argparse
import numpy as np
import scipy.stats

class CDFentry:
    def __init__(self):
        cdf_ = 0
        val_ = 0

class ConstantVariable:
    def __init__(self,val):
        self.val = val
    def value(self):
        return self.val

class UniformRandomVariable:
    def __init__(self):
        self.min_ = 0.0
        self.max_ = 1.0

    def value(self):
        unif0_1 = random.random()
        return self.min_ + (self.max_ - self.min_) * unif0_1

class ExponentialRandomVariable:
    def __init__(self, avg):
        self.avg_ = avg
        self.urv = UniformRandomVariable()
    def value(self):
        return -1.0 * self.avg_ * math.log(self.urv.value())


class EmpiricalRandomVariable:

    def __init__(self, filename, smooth, packetsize, headersize):
        self.smooth = smooth
        self.minCDF_ = 0
        self.maxCDF_ = 1
        self.table_ = []
        self.packetSize_ = packetsize
        self.headerSize_ = headersize
        for i in range(65536):
            self.table_.append(CDFentry())
        if(filename != ""):
            self.loadCDF(filename)

    def loadCDF(self, filename):
        numEntry_ = 0
        prev_cd = 0
        prev_sz = 1
        w_sum = 0
        file = open(filename, "r")
        f = file.readlines()
        for line in f:
            values = line.split()
            self.table_[numEntry_].val_ = float(values[0])
            self.table_[numEntry_].cdf_ = float(values[1])
            self.table_[numEntry_].cdf_ = float(values[2])
            freq = self.table_[numEntry_].cdf_ - prev_cd
            flow_sz = 0
            if self.smooth:
                flow_sz = (self.table_[numEntry_].val_ + prev_sz) / 2.0
            else: 
                flow_sz = self.table_[numEntry_].val_
            w_sum += freq * flow_sz
            prev_cd = self.table_[numEntry_].cdf_
            prev_sz = self.table_[numEntry_].val_
            numEntry_ += 1

        self.mean_flow_size = w_sum * float(self.packetSize_ - self.headerSize_);
        file.close()
        self.numEntry_ = numEntry_
        return numEntry_

    def lookup(self, u):
        lo = 1
        hi = self.numEntry_ - 1 
        mid = 0
        if u <= self.table_[0].cdf_:
            return 0
        while lo < hi:
            mid = (lo + hi) / 2
            if u > self.table_[mid].cdf_:
                lo = mid + 1
            else:
                hi = mid 
        return lo
    
    def interpolate(self, x, x1, y1, x2, y2):
        value = y1 + (x - x1) * (y2 - y1) / (x2 - x1)
        return value

    def value(self):
        if self.numEntry_ <= 0:
            return 0
        u = random.random()
        mid = self.lookup(u)
        if mid != 0 and u < self.table_[mid].cdf_:
            return self.interpolate(u, self.table_[mid-1].cdf_, self.table_[mid-1].val_,
                self.table_[mid].cdf_, self.table_[mid].val_)
        return self.table_[mid].val_


