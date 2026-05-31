-- ============================================================
-- JOB 3.1 — Statistiche Compagnie Aeree (HiveQL Universale)
-- ============================================================

-- Aumento della memoria per i task MapReduce locali per evitare l'errore "return code 2"
SET mapreduce.map.memory.mb=2048;
SET mapreduce.map.java.opts=-Xmx1638m;
SET mapreduce.reduce.memory.mb=2048;
SET mapreduce.reduce.java.opts=-Xmx1638m;
SET hive.exec.reducers.bytes.per.reducer=134217728;

CREATE DATABASE IF NOT EXISTS airline_db;
USE airline_db;

-- ─── 1. Tabella sorgente con doppia partizione (Punta a HDFS o S3) ──────
DROP TABLE IF EXISTS flights_raw_${hivevar:SUBSET};
CREATE EXTERNAL TABLE flights_raw_${hivevar:SUBSET} (
    OP_UNIQUE_CARRIER   STRING,
    ORIGIN              STRING,
    ARR_DELAY           DOUBLE,
    CANCELLED           DOUBLE
)
PARTITIONED BY (YEAR INT, MONTH INT)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}';

-- ─── 2. Gestione delle partizioni YEAR+MONTH ────────────────
ALTER TABLE flights_raw_${hivevar:SUBSET} ADD IF NOT EXISTS 
  PARTITION (YEAR=2024, MONTH=1)  LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=1'
  PARTITION (YEAR=2024, MONTH=2)  LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=2'
  PARTITION (YEAR=2024, MONTH=3)  LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=3'
  PARTITION (YEAR=2024, MONTH=4)  LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=4'
  PARTITION (YEAR=2024, MONTH=5)  LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=5'
  PARTITION (YEAR=2024, MONTH=6)  LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=6'
  PARTITION (YEAR=2024, MONTH=7)  LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=7'
  PARTITION (YEAR=2024, MONTH=8)  LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=8'
  PARTITION (YEAR=2024, MONTH=9)  LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=9'
  PARTITION (YEAR=2024, MONTH=10) LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=10'
  PARTITION (YEAR=2024, MONTH=11) LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=11'
  PARTITION (YEAR=2024, MONTH=12) LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}/YEAR=2024/MONTH=12';

-- Verifica del caricamento record per i log di Beeline
SELECT COUNT(*) AS totale_record FROM flights_raw_${hivevar:SUBSET};


-- ─── 3. Tabella risultato (Esterna, per salvare i dati su HDFS o S3 stabilmente) ───
DROP TABLE IF EXISTS airline_stats_${hivevar:SUBSET};
CREATE EXTERNAL TABLE airline_stats_${hivevar:SUBSET} (
    airline                 STRING,
    airport                 STRING,
    total_flights           BIGINT,
    min_arr_delay           DOUBLE,
    max_arr_delay           DOUBLE,
    avg_arr_delay           DOUBLE,
    cancellation_rate_pct   DOUBLE,
    active_months           ARRAY<INT>
)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}/data/outputs/job31/hive_${hivevar:SUBSET}';


-- ─── 4. Query principale di aggregazione distribuita ──────────────────────
INSERT OVERWRITE TABLE airline_stats_${hivevar:SUBSET}
SELECT
    OP_UNIQUE_CARRIER                               AS airline,
    ORIGIN                                          AS airport,
    COUNT(*)                                        AS total_flights,
    MIN(ARR_DELAY)                                  AS min_arr_delay,
    MAX(ARR_DELAY)                                  AS max_arr_delay,
    ROUND(AVG(ARR_DELAY), 2)                        AS avg_arr_delay,
    ROUND(SUM(CANCELLED) / COUNT(*) * 100, 2)       AS cancellation_rate_pct,
    sort_array(collect_set(MONTH))                  AS active_months
FROM flights_raw_${hivevar:SUBSET}
GROUP BY OP_UNIQUE_CARRIER, ORIGIN
ORDER BY airline, airport;

-- ─── 5. Stampa delle prime 10 righe richieste dal report finalizzato ──────
SELECT * FROM airline_stats_${hivevar:SUBSET} LIMIT 10;
