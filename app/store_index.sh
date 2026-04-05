#!/bin/bash
echo "Storing index data in Cassandra/ScyllaDB tables"

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

python3 app.py
