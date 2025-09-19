#!/usr/bin/env bash
set -e

SPARK_HOME=$HOME/spark
MASTER_URL="spark://localhost:7077"
NUM_WORKERS=4
WORKER_CORES=2
WORKER_MEM=4g

echo "=== Starting Spark Master ==="
$SPARK_HOME/sbin/start-master.sh

echo "=== Starting ${NUM_WORKERS} Spark Workers ==="
for i in $(seq 1 $NUM_WORKERS); do
  $SPARK_HOME/sbin/start-worker.sh $MASTER_URL \
    --cores $WORKER_CORES \
    --memory $WORKER_MEM
done

echo "=== Spark Cluster Started ==="
echo "Master UI: http://localhost:8080"
echo "Connect to: $MASTER_URL"
