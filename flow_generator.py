import argparse
import heapq
import numpy as np
import os
import sys
import scipy.stats

from random_variable import *

def poissonFlowGenerator(packet_size, header_size, num_flows, num_hosts, bandwidth, load, filename, smooth):
    pq = []
    nv_bytes = EmpiricalRandomVariable(filename, smooth,packet_size,header_size)

    mean_flow_size = nv_bytes.mean_flow_size
    print nv_bytes.mean_flow_size
    lmda = bandwidth * load / (mean_flow_size * 8.0 / (packet_size - header_size) * packet_size)
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
        size = int(size) * (packet_size - header_size)
        next_time = time + nv_intarr.value()
        flows.append([time, src, dst, flow_id, size])
        heapq.heappush(pq,[next_time, src, dst, nv_bytes, nv_intarr])

    return flows

def poissonFlowGeneratorMaxLimits(packet_size, header_size, num_flows, num_hosts, bandwidth, load, filename, smooth,fcnt):
    pq = []
    nv_bytes = EmpiricalRandomVariable(filename, smooth,packet_size,header_size)

    mean_flow_size = nv_bytes.mean_flow_size
    lmda = bandwidth * load / (mean_flow_size * 8.0 / (packet_size - header_size) * packet_size)
    lambda_per_host = lmda / (num_hosts - 1)

    src_host_flow_cnt = []
    dst_host_flow_cnt = []

    for i in range(num_hosts):
        src_host_flow_cnt.append(0)
        dst_host_flow_cnt.append(0)

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
        size = int(size) * (packet_size - header_size)

        if( (src_host_flow_cnt[src] < fcnt) and (dst_host_flow_cnt[dst] < fcnt) ):
            next_time = time + nv_intarr.value()
            flows.append([time, src, dst, flow_id, size])
            heapq.heappush(pq,[next_time, src, dst, nv_bytes, nv_intarr])
            src_host_flow_cnt[src] += 1
            dst_host_flow_cnt[dst] += 1
        else:
            pass
        
    return flows

def write_to_file(packet_size, header_size, output_file, flows):
    file = open(output_file, "w")
    for f in flows:
        s = ""
        s += "{0}, {1}, {2}, {3}, {4}\n".format(f[0], f[1], f[2], f[3], f[4])
        file.write(s)
    file.close()

def write_to_file_dcqcn(packet_size, header_size, output_file, flows, num_flows):
    file = open(output_file, "w")
    file.write(str(num_flows)+'\n')
    for f in flows:
        s = ""
        s += "{0} {1} 3 {2} {3} 10.5\n".format(f[1], f[2], f[4]/(packet_size - header_size), f[0])
        file.write(s)
    file.close()

def test_avg_load(num_hosts, flows, bandwidth, test_fcnt, fcnt=128):
    #flows.append([time, src, dst, flow_id, size])
    data_sent_per_port = []
    min_arrival_time = []
    max_arrival_time = []
    load_per_port = []
    for i in range(num_hosts):
        data_sent_per_port.append(0)
        min_arrival_time.append(100000)
        max_arrival_time.append(-1)
        load_per_port.append(0.0)
    for f in flows:
        arrival_time = f[0]
        src = f[1]
        dst = f[2]
        flow_id = f[3]
        size_bytes = f[4]
        data_sent_per_port[src] += size_bytes
        if(arrival_time < min_arrival_time[src]):
            min_arrival_time[src] = arrival_time
        if(arrival_time > max_arrival_time[src]):
            max_arrival_time[src] = arrival_time

    for i in range(num_hosts):
        load_per_port[i] = float(data_sent_per_port[i] * 8) / float(bandwidth * (max_arrival_time[i] - min_arrival_time[i]))
    print "Load across ports: avg: ", sum(load_per_port)/len(load_per_port), " max: ", max(load_per_port), " min: ", min(load_per_port)

    
    if(test_fcnt == True):
        src_flow_cnt = []
        dst_flow_cnt = []
        for i in range(num_hosts):
            src_flow_cnt.append(0)
            dst_flow_cnt.append(0)
        
        for f in flows:
            src = f[1]
            dst = f[2]
            src_flow_cnt[src] += 1
            dst_flow_cnt[dst] += 1

        if(max(src_flow_cnt) > fcnt):
            print " Exceeded max src flow count: ", max(src_flow_cnt)
        elif(max(dst_flow_cnt) > fcnt):
            print " Exceeded max dst flow count: ", max(dst_flow_cnt)
        else:
            print "fcnt test passed!"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p','--packetsize',default=1000,
        help='size of packets in bytes')
    parser.add_argument('-hs','--headersize',default=0,
        help='size of packet header in bytes')
    parser.add_argument('-s', '--scale', default=144,
        help='the number of nodes')
    parser.add_argument('-f', '--flows', default=10000,
        help='the number of flows')
    parser.add_argument('-c', '--cdf', default='CDF_dctcp.txt',
        help='flow size cdf')
    parser.add_argument('-b', '--bandwidth', default=40000000000,
        help='bandwidth default: 40Gbps')
    parser.add_argument('-l', '--load', default=0.8,
        help='the network load')
    parser.add_argument('-O', '--output', default='flows.txt',
        help='the output file folder')
    parser.add_argument('-x','--fcnt',default=128,
        help='fcnt variable in DCQCN simulator default: 128')
    parser.add_argument('-t','--type', default='P',
        help='type of workload generator [P: Poisson, PD: Poisson for DCQCN, ... more to be added soon]')

    args = parser.parse_args()
    packet_size = int(args.packetsize)
    header_size = int(args.headersize)
    num_hosts = int(args.scale)
    num_flows = int(args.flows)
    load = float(args.load)
    cdf = args.cdf
    bandwidth = float(args.bandwidth)
    output = str(args.output)

    fcnt = int(args.fcnt)
    gen_type = str(args.type)
    # FILE = str(scale)+'-'+str(load)+'-'+str(stages)+'-'+str(args.incast)+'-'+str(args.outcast)+'-'+ str(data_dist)
    # output_file = output + FILE + '.txt'
    if(gen_type == 'P'):
        flows = poissonFlowGenerator(packet_size, header_size, num_flows, num_hosts, bandwidth, load, cdf, 1)
        test_avg_load(num_hosts,flows,bandwidth,False)
        write_to_file(packet_size, header_size, output, flows)
    elif(gen_type == 'PD'):
        assert(num_flows <= (num_hosts * fcnt))
        flows = poissonFlowGeneratorMaxLimits(packet_size, header_size, num_flows, num_hosts, bandwidth, load, cdf, 1,fcnt)
        test_avg_load(num_hosts,flows,bandwidth,True,fcnt)
        write_to_file_dcqcn(packet_size, header_size, output, flows, num_flows)
    else:
        print "Invalid generator type"

    
if __name__ == '__main__':
    main()