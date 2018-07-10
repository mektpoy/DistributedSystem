#!/bin/bash

set -x
hdfs dfs -rm -r -f lab2
hadoop jar GraphBuilder.jar GraphBuilder -Dmapreduce.job.queuename=test /data/wiki lab2
sleep 3
hdfs dfs -get lab2 .
