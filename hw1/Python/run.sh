#!/bin/sh
hadoop fs -rm -r python_citation

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -file mapper2.py reducer.py \
    -mapper 'mapper2.py 1 4' \
    -reducer reducer.py \
    -input /data/patents/apat63_99.txt \
    -output python_citation
