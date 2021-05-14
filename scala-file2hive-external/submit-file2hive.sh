#!/bin/bash
# Wrapper script to launch loadfile2hive

if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <input_file_path> <input_file_type> <target_table_name> <target_output_format>"
    exit 1
fi

file_path=$1
file_type=$2
table_name=$3
output_format=$4

spark-submit  \
  --class com.cloudera.spark.examples.loadfile2hive \
  --master=yarn  \
  --deploy-mode=cluster \
  --executor-memory 2G  \
  --driver-memory 2G  \
  --num-executors 6 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  file2hive-1.0.jar ${file_path} ${file_type} ${table_name} ${output_format}
