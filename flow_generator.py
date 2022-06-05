import argparse
import heapq
import numpy as np
import os
import sys
import scipy.stats
import random
from random_variable import *

# def convert_to_homa_format():
TIME = 0
SIZE = 1
SRC = 2
DST = 3
TYPE = 4
PG = 5
def convert_to_homa_format(flows):
    i = 0
    last_time = 0.0
    for flow in flows:
        start_time = flow[TIME] - 1.0
        output = "{} {} {} {}".format(flow[SRC], flow[DST],  int(flow[SIZE]), start_time - last_time)
        last_time = start_time
        print output
        i += 1

def convert_to_ndp_format(flows):
    i = 0
    for flow in flows:
        output = "{},{},{},{},{}".format(i, flow[SRC], flow[DST], int(flow[SIZE]), flow[TIME] - 1.0)
        print output
        i += 1
def convert_to_hpcc_format(flows):
    i = 0
    print len(flows)
    for flow in flows:
        print "{} {} {} {} 100 {} {} {}".format(i, flow[SRC], flow[DST], int(flow[PG]) , int(flow[SIZE]) / 1460 * 1500, flow[TIME], flow[TYPE])
        i += 1

def convert_to_pim_format(flows):
    i = 0
    for flow in flows:
        output = "{} {} {} {} {} {} {} {} {}".format(i, flow[0], -1, -1, int(flow[1]) / 1460, -1, -1, flow[2], flow[3])
        print output
        i += 1

def poissonFlowGenerator(num_flows, num_hosts, bandwidth, load, filename, smooth, is_tcp = 0, pg = 3):
    pq = []
    nv_bytes = EmpiricalRandomVariable(filename, smooth)
    flows = []
    if load == 0.0:
        return flows, 0 
    mean_flow_size = nv_bytes.mean_flow_size
    lmda = bandwidth * load / (mean_flow_size * 8.0 / 1460 * 1500)
    lambda_per_host = lmda / (num_hosts - 1)

    nv_intarr = ExponentialRandomVariable(1.0 / lambda_per_host)
    for i in range(num_hosts):
        for j in range(num_hosts):
            if i != j:
                first_flow_time = 1.0 + nv_intarr.value()
                heapq.heappush(pq,[first_flow_time, i, j, nv_bytes, nv_intarr])

    next_time = 0
    finish_time = 0
    while len(flows) < num_flows:
        element = heapq.heappop(pq)
        flow_id = len(flows)
        time = element[0]
        finish_time = time
        src = element[1]
        dst = element[2]
        nv_bytes = element[3]
        nv_intarr = element[4]
        size = nv_bytes.value() + 0.5 # truncate(val + 0.5) equivalent to round to nearest int
        if (size > 2500000):
            size = 2500000
        size = int(size) * 1460
        next_time = time + nv_intarr.value()
        flows.append([time, size,  src, dst, is_tcp, pg])
        heapq.heappush(pq,[next_time, src, dst, nv_bytes, nv_intarr])

    return flows, finish_time

def poissonFlowIncastGenerator(num_flows, num_hosts, bandwidth, load, filename, smooth, stopTime, incast_degree, incast_flow_size,is_tcp = 0, pg = 2):
    pq = []
    flows = []
    if load == 0:
        return flows
    mean_flow_size = incast_degree * incast_flow_size
    lmda = bandwidth * load / (mean_flow_size * 8.0 / 1460 * 1500)
    # lambda_per_host = lmda / (num_hosts - 1)
    lambda_per_host = lmda
    nv_intarr = ExponentialRandomVariable(1.0 / lambda_per_host)
    for i in range(num_hosts):
        first_flow_time = 1.0 + nv_intarr.value()
        heapq.heappush(pq,[first_flow_time, i, mean_flow_size, nv_intarr])
    next_time = 0
    while 1:
        element = heapq.heappop(pq)
        flow_id = len(flows)
        time = element[0]
        src = element[1]
        # nv_bytes = element[2]
        nv_intarr = element[3]
        # size = nv_bytes.value() + 0.5 # truncate(val + 0.5) equivalent to round to nearest int
        # if (size > 2500000):
        #     size = 2500000
        if time > stopTime:
            break
        size = int(mean_flow_size) / incast_degree / 1500 * 1460
        next_time = time + nv_intarr.value()
        for i in range(incast_degree):
            dst = random.randint(1, num_hosts - 1)
            while dst == src:
                dst = random.randint(1, num_hosts - 1)
            flows.append([time,size,dst, src, is_tcp, pg])
        heapq.heappush(pq,[next_time, src, mean_flow_size, nv_intarr])

    return flows


def write_to_file(output_file, flows):
    file = open(output_file, "w")
    for f in flows:
        s = ""
        s += "{0}, {1}, {2}, {3}, {4}\n".format(f[0], f[1], f[2], f[3], f[4])
        file.write(s)
    file.close()

def main():
    random.seed(30)
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--scale', default=144,
        help='the number of nodes')
    parser.add_argument('-f', '--flows', default=400000,
        help='the number of flows')
    parser.add_argument('-c', '--cdf', default='imc10',
        help='flow size cdf')
    parser.add_argument('-b', '--bandwidth', default=100000000000,
        help='bandwidth default: 100Gbps')
    parser.add_argument('-l', '--load', default=0.8,
        help='the network load')
    parser.add_argument('-il', '--iload', default=0.02,
        help='the network load')
    parser.add_argument('-tl', '--tcpload', default=0.0,
        help='the network load of TCP')
    parser.add_argument('-F', '--format', default='pim',
        help='the output file format: pim, homa, ndp, hpcc')
    args = parser.parse_args()
    num_hosts = int(args.scale)
    num_flows = int(args.flows)
    load = float(args.load)
    cdf = args.cdf
    bandwidth = float(args.bandwidth)
    form = str(args.format)
    iload = float(args.iload)
    tcp_load = float(args.tcpload)
    flows = []
    # FILE = str(scale)+'-'+str(load)+'-'+str(stages)+'-'+str(args.incast)+'-'+str(args.outcast)+'-'+ str(data_dist)
    # output_file = output + FILE + '.txt'
    flows, next_time = poissonFlowGenerator(num_flows, num_hosts, bandwidth, load, "CDF_{}.txt".format(cdf), 1, 0, 3)
    flows2 = poissonFlowIncastGenerator(num_flows, num_hosts, bandwidth, iload, "CDF_{}.txt".format(cdf), 1, next_time, 50, 64000, 1, 2)
    tcp_flow, next_time = poissonFlowGenerator(num_flows, num_hosts, bandwidth, tcp_load, "CDF_{}.txt".format(cdf), 1, 1, 1)
    for f in flows2:
        flows.append(f)
    for f in tcp_flow:
        flows.append(f)
    flows = sorted(flows)
    if form == "pim":
        convert_to_pim_format(flows)
    if form == "ndp":
        convert_to_ndp_format(flows)
    if form == "homa":
        convert_to_homa_format(flows)
    if form == "hpcc":
        # load = load + tcp_load
        if not os.path.exists("result/mix_workload/hpcc/{}_{}".format(cdf, int(load * 10))):
            os.makedirs("result/mix_workload/hpcc/{}_{}".format(cdf, int(load * 10)))
        convert_to_hpcc_format(flows)
    # write_to_file(output, flows)
if __name__ == '__main__':
    main()
