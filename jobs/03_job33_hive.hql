-- ============================================================
-- JOB 3.3 — Ranking Anomalie (HiveQL Universale)
-- ============================================================
-- Uso via Beeline:
--   beeline -u jdbc:hive2://localhost:10000 -n hive \
--       --hivevar BASE_PATH=hdfs://namenode:9000 \
--       --hivevar SUBSET=pct_050 \
--       --hivevar INPUT_DIR=/data/subsets/pct_050 \
--       < jobs/03_job33_hive.hql

SET mapreduce.map.memory.mb=2048;
SET mapreduce.map.java.opts=-Xmx1638m;
SET mapreduce.reduce.memory.mb=2048;
SET mapreduce.reduce.java.opts=-Xmx1638m;
SET hive.exec.reducers.bytes.per.reducer=134217728;

CREATE DATABASE IF NOT EXISTS airline_db;
USE airline_db;

-- ─── 1. Tabella sorgente ──────────────────────────────────────────────────
DROP TABLE IF EXISTS flights_raw33_${hivevar:SUBSET};
CREATE EXTERNAL TABLE flights_raw33_${hivevar:SUBSET} (
    OP_UNIQUE_CARRIER   STRING,
    ORIGIN              STRING,
    DEP_DELAY           DOUBLE,
    ARR_DELAY           DOUBLE,
    CANCELLED           DOUBLE
)
PARTITIONED BY (YEAR INT, MONTH INT)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}';

-- ─── 2. Aggiunta partizioni YEAR+MONTH ────────────────────────────────────
ALTER TABLE flights_raw33_${hivevar:SUBSET} ADD IF NOT EXISTS
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

-- Verifica record
SELECT COUNT(*) AS totale_record FROM flights_raw33_${hivevar:SUBSET};

-- ─── 3. Tabella risultato ─────────────────────────────────────────────────
DROP TABLE IF EXISTS job33_ranking_${hivevar:SUBSET};
CREATE EXTERNAL TABLE job33_ranking_${hivevar:SUBSET} (
    airport                 STRING,
    airline                 STRING,
    flight_count            BIGINT,
    avg_dep_delay           DOUBLE,
    avg_arr_delay           DOUBLE,
    cancellation_rate_pct   DOUBLE,
    airport_avg_dep_delay   DOUBLE,
    airport_total_flights   BIGINT,
    dep_delay_diff          DOUBLE,
    rank_in_airport         INT,
    anomaly_label           STRING
)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}/data/outputs/job33/hive_${hivevar:SUBSET}';

-- ─── 4. Query principale ──────────────────────────────────────────────────
WITH airline_airport_stats AS (
    SELECT
        ORIGIN                                                              AS airport,
        OP_UNIQUE_CARRIER                                                   AS airline,
        COUNT(*)                                                            AS flight_count,
        ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEP_DELAY ELSE NULL END), 2) AS avg_dep_delay,
        ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARR_DELAY ELSE NULL END), 2) AS avg_arr_delay,
        ROUND(SUM(CANCELLED) / COUNT(*) * 100.0, 2)                         AS cancellation_rate_pct
    FROM flights_raw33_${hivevar:SUBSET}
    WHERE ORIGIN IS NOT NULL AND TRIM(ORIGIN) != ''
      AND OP_UNIQUE_CARRIER IS NOT NULL AND TRIM(OP_UNIQUE_CARRIER) != ''
    GROUP BY ORIGIN, OP_UNIQUE_CARRIER
),
airport_global_avg AS (
    SELECT
        ORIGIN                                                              AS airport,
        ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEP_DELAY ELSE NULL END), 2) AS airport_avg_dep_delay,
        COUNT(*)                                                            AS airport_total_flights
    FROM flights_raw33_${hivevar:SUBSET}
    WHERE ORIGIN IS NOT NULL AND TRIM(ORIGIN) != ''
    GROUP BY ORIGIN
),
ranked AS (
    SELECT
        s.airport,
        s.airline,
        s.flight_count,
        s.avg_dep_delay,
        s.avg_arr_delay,
        s.cancellation_rate_pct,
        a.airport_avg_dep_delay,
        a.airport_total_flights,
        ROUND(s.avg_dep_delay - a.airport_avg_dep_delay, 2)                AS dep_delay_diff,
        RANK() OVER (
            PARTITION BY s.airport
            ORDER BY s.avg_dep_delay ASC
        )                                                                  AS rank_in_airport
    FROM airline_airport_stats s
    JOIN airport_global_avg a ON s.airport = a.airport
)
INSERT OVERWRITE TABLE job33_ranking_${hivevar:SUBSET}
SELECT
    airport,
    airline,
    flight_count,
    COALESCE(avg_dep_delay, 0.0)         AS avg_dep_delay,
    COALESCE(avg_arr_delay, 0.0)         AS avg_arr_delay,
    cancellation_rate_pct,
    COALESCE(airport_avg_dep_delay, 0.0) AS airport_avg_dep_delay,
    airport_total_flights,
    COALESCE(dep_delay_diff, 0.0)        AS dep_delay_diff,
    rank_in_airport,
    CASE
        WHEN dep_delay_diff >  15.0 THEN 'ANOMALO_PEGGIORE'
        WHEN dep_delay_diff < -15.0 THEN 'ANOMALO_MIGLIORE'
        ELSE                             'NELLA_NORMA'
    END AS anomaly_label
FROM ranked
ORDER BY airport ASC, rank_in_airport ASC;

-- ─── 5. Stampa prime 10 righe richieste ───────────────────────────────────
SELECT * FROM job33_ranking_${hivevar:SUBSET} LIMIT 10;
