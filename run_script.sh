#!/bin/bash

loads=(71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89)
# loads=(40 50 60 70 80)
# loads=(90 91 92 93 94 95 96 97 98 99)
algos=(hpcc)
calc(){ awk "BEGIN { print "$*" }"; }
pids=()

OUTPUT_FOLDER=results/workload
TRACE=$1
mkdir -p $OUTPUT_FOLDER
for i in ${!algos[*]}
do 
	mkdir -p $OUTPUT_FOLDER/${algos[$i]}
	for index in ${!loads[*]};
	do
   	    load=${loads[$index]}
	    algo=${algos[$i]}
	    # echo conf_"$algo"_dctcp_$load.txt
	    # echo "$OUTPUT_FOLDER"/result_"$algo"_dctcp_"$load".txt
	    echo "python flow_generator.py -b 400000000000 -s 128 -il 0 -tl 0 -l 0.${load} -F $algo -c ${TRACE} > $OUTPUT_FOLDER/$algo/trace_${TRACE}_${load}.txt"
	    python3 flow_generator.py -b 400000000000 -s 32 -f 1000000 -il 0 -tl 0 -l 0.${load} -F $algo -c ${TRACE} > $OUTPUT_FOLDER/$algo/trace_${TRACE}_${load}.txt
	    #	nohup ./batch_simulate_sflow.py -P $p -F ../../../data/ -t ${threshold[$index]} -i 10 -N 1000 -s 1 -l results/conext18/flows/percentage-${percentage[$index]}.log &
	    pids[${index}]=$!
	done
	for pid in ${pids[*]}; 
	do
    	  wait $pid
        done
    pids=()
done
