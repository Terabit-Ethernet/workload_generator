import argparse
import heapq
import numpy as np
import os
import sys
import scipy.stats

from random_variable import *

def poissonFlowGenerator(num_flows, num_hosts, bandwidth, load, filename, smooth):
    pq = []
    nv_bytes = EmpiricalRandomVariable(filename, smooth)

    mean_flow_size = nv_bytes.mean_flow_size
    lmda = bandwidth * load / (mean_flow_size * 8.0 / 1460 * 1500)
    lambda_per_host = lmda / (num_hosts - 1)

    nv_intarr = ExponentialRandomVariable(1.0 / lambda_per_host)
    for i in range(num_hosts):
        for j in range(num_hosts):
            if i != j:
                first_flow_time = 1.0 + nv_intarr.value()
                heapq.heappush(pq,[first_flow_time, i, j, nv_bytes, nv_intarr])

    flows = []
    while len(flows) < num_flows:
        element = heapq.heappop(pq)
        flow_id = len(flows)
        time = element[0]
        src = element[1]
        dst = element[2]
        nv_bytes = element[3]
        nv_intarr = element[4]
        size = nv_bytes.value() + 0.5 # truncate(val + 0.5) equivalent to round to nearest int
        if (size > 2500000):
            size = 2500000
        size = int(size) * 1460
        next_time = time + nv_intarr.value()
        flows.append([time, src, dst, flow_id, size])
        heapq.heappush(pq,[next_time, src, dst, nv_bytes, nv_intarr])

    return flows

def write_to_file(output_file, flows):
    file = open(output_file, "w")
    for f in flows:
        s = ""
        s += "{0}, {1}, {2}, {3}, {4}\n".format(f[0], f[1], f[2], f[3], f[4])
        file.write(s)
    file.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--scale', default=144,
        help='the number of nodes')
    parser.add_argument('-f', '--flows', default=200000,
        help='the number of flows')
    parser.add_argument('-c', '--cdf', default='CDF_dctcp.txt',
        help='flow size cdf')
    parser.add_argument('-b', '--bandwidth', default=10000000000,
        help='bandwidth default: 10000000000Gbps')
    parser.add_argument('-l', '--load', default=0.8,
        help='the network load')
    parser.add_argument('-O', '--output', default='flows.txt',
        help='the output file folder')

    args = parser.parse_args()
    num_hosts = int(args.scale)
    num_flows = int(args.flows)
    load = float(args.load)
    cdf = args.cdf
    bandwidth = float(args.bandwidth)
    output = str(args.output)
    # FILE = str(scale)+'-'+str(load)+'-'+str(stages)+'-'+str(args.incast)+'-'+str(args.outcast)+'-'+ str(data_dist)
    # output_file = output + FILE + '.txt'
    flows = poissonFlowGenerator(num_flows, num_hosts, bandwidth, load, cdf, 1)
    write_to_file(output, flows)
if __name__ == '__main__':
    main()