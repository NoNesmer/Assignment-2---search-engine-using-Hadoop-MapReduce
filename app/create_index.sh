#!/bin/bash

echo "Create index using MapReduce pipelines"

INPUT_PATH=${1:-"/input/data"}
echo "Input path: $INPUT_PATH"

# clean previous outputs
hdfs dfs -rm -r -f /indexer/index
hdfs dfs -rm -r -f /indexer/vocab

# Pipeline 1: build inverted index (term -> doc_id, tf, dl)
echo "=== Pipeline 1: Building inverted index ==="
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input $INPUT_PATH \
    -output /indexer/index \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py

echo "Pipeline 1 done."
hdfs dfs -ls /indexer/index

# Pipeline 2: compute document frequencies for vocabulary
echo "=== Pipeline 2: Computing document frequencies ==="
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /indexer/index \
    -output /indexer/vocab \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py

echo "Pipeline 2 done."
hdfs dfs -ls /indexer/vocab

echo "Index creation complete!"
