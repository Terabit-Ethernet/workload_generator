#!/bin/bash

loads=(5 6 7 8 9)
algos=(pim)
calc(){ awk "BEGIN { print "$*" }"; }
pids=()

OUTPUT_FOLDER=result/bursty_workload
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
	    echo python flow_generator.py -l 0.${load} -F $algo -c ${TRACE}
	    python flow_generator.py -l 0.${load} -F $algo -c ${TRACE} > $OUTPUT_FOLDER/$algo/trace_${TRACE}_${load}.txt
	    #	nohup ./batch_simulate_sflow.py -P $p -F ../../../data/ -t ${threshold[$index]} -i 10 -N 1000 -s 1 -l results/conext18/flows/percentage-${percentage[$index]}.log &
	    pids[${index}]=$!
	done
	for pid in ${pids[*]}; 
	do
    	  wait $pid
        done
    pids=()
done
