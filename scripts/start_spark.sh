#!/bin/bash

if ! nc -z localhost 8081; then
  echo "Starting Spark Master..."
  cd /opt/spark && ./sbin/start-master.sh --port 8081

  sleep 1
else
  echo "Spark master is already running."
fi

# Check and start Spark worker
if ! jps | grep -q Worker; then
  echo "Starting Spark Worker..."
  cd /opt/spark && ./sbin/start-worker.sh spark://localhost:7077

  sleep 1
else
  echo "Spark worker is already running."
fi