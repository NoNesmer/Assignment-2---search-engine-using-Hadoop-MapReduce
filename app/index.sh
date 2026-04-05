#!/bin/bash
echo "Running full indexing pipeline"

INPUT_PATH=${1:-"/input/data"}

echo "Step 1: Creating index in HDFS via MapReduce"
bash create_index.sh "$INPUT_PATH"

echo "Step 2: Storing index in Cassandra"
bash store_index.sh

echo "Indexing complete!"
