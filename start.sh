#!/bin/bash
killall ingester orchestrator load-generator 2>/dev/null || true
nohup ./bin/ingester > ingester.log 2>&1 &
sleep 1
nohup ./bin/orchestrator > orchestrator.log 2>&1 &
sleep 1
nohup ./bin/load-generator > load-generator.log 2>&1 &
echo "Services started."
