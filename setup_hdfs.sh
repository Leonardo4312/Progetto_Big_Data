#!/bin/bash
# setup_hdfs.sh — carica il dataset su HDFS e prepara le directory di progetto
# Esegui DOPO che i container sono UP: bash setup_hdfs.sh

set -e

echo "=== [1/4] Attesa NameNode online ==="
until docker exec namenode hdfs dfsadmin -report &> /dev/null; do
  echo "  NameNode non ancora pronto, attendo 5s..."
  sleep 5
done

echo "=== [2/4] Creazione directory HDFS ==="
docker exec namenode hdfs dfs -mkdir -p /user/bigdata/flight2024/raw
docker exec namenode hdfs dfs -mkdir -p /user/bigdata/flight2024/cleaned
docker exec namenode hdfs dfs -mkdir -p /user/bigdata/flight2024/output/job31
docker exec namenode hdfs dfs -mkdir -p /user/bigdata/flight2024/output/job32
docker exec namenode hdfs dfs -mkdir -p /user/bigdata/flight2024/output/job33
docker exec namenode hdfs dfs -chmod -R 777 /user/bigdata

echo "=== [3/4] Upload CSV su HDFS ==="
# Il file CSV deve trovarsi in ./data/flights_2024.csv sulla macchina host
docker exec namenode hdfs dfs -put -f /data/flights_2024.csv /user/bigdata/flight2024/raw/

echo "=== [4/4] Verifica upload ==="
docker exec namenode hdfs dfs -ls /user/bigdata/flight2024/raw/
docker exec namenode hdfs dfs -du -h /user/bigdata/flight2024/raw/

echo ""
echo "✅ Setup completato. Dataset disponibile in HDFS."
echo "   HDFS Web UI: http://localhost:9870"
echo "   Spark Web UI: http://localhost:8080"
echo "   HiveServer2 Web UI: http://localhost:10002"
