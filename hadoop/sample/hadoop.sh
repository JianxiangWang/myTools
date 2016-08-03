#!/bin/bash

K=1000

INPUT="/app/ps/spider/kg-value/wangjianxiang01/data/sample_test_in"
OUTPUT="/app/ps/spider/kg-value/wangjianxiang01/data/sample_test_out"

hadoop fs -rmr ${OUTPUT}

hadoop streaming \
    -input "${INPUT}"  \
    -output "${OUTPUT}" \
    -mapper "mapper.py ${K}" \
    -reducer "reducer.py ${K}" \
    -file "mapper.py" \
    -file "reducer.py" \
    -cacheArchive "/app/ps/spider/kg-value/wangjianxiang01/python.tar.gz#." \
    -jobconf mapred.job.priority="VERY_HIGH" \
    -jobconf mapred.textoutputformat.ignoreseparator=true \
    -jobconf stream.num.map.output.key.fields=1 \
    -jobconf mapred.output.compress=true \
    -jobconf mapred.compress.map.output=true \
    -jobconf mapred.map.tasks=10 \
    -jobconf mapred.job.map.capacity=400 \
    -jobconf mapred.reduce.tasks=1 \
    -jobconf mapred.job.reduce.capacity=400 \
    -jobconf mapred.job.name="wangjianxiang_sampling"


#"-jobconf mapred.output.compress=true \
