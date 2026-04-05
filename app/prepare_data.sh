#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

echo "Putting data files to HDFS /data/"
hdfs dfs -rm -r -f /data
hdfs dfs -put -f data /

echo "Files in HDFS /data/:"
hdfs dfs -ls /data/ | wc -l

echo "Preparing input data for MapReduce..."
spark-submit prepare_data.py

echo "Verifying data in HDFS:"
hdfs dfs -ls /input/data

echo "Done data preparation!"
