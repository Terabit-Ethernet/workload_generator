# Workload Generator
The generator uses Python 3.
To run the generator for each workload,
```
./run_script.sh imc10
./run_script.sh websearch
./run_script.sh datamining
```

The trace files are in `results/workload/ndp/` and for each workload, the generator generates 5 trace files with 0.5, 0.6, 0.7, 0.8, and 0.9 load as specified in `run_script.sh`. 

To get traces for different simulators, change `algos=(ndp)` to the one you need. The current generator supports dcPIM, NDP, HPCC and Homa. 
