-- ============================================================
-- JOB 3.2 — Report Ritardi e Cause per Aeroporto (HiveQL Universale)
-- ============================================================
-- Uso via Beeline:
--   beeline -u jdbc:hive2://localhost:10000 -n hive \
--       --hivevar BASE_PATH=hdfs://namenode:9000 \
--       --hivevar SUBSET=pct_050 \
--       --hivevar INPUT_DIR=/data/subsets/pct_050 \
--       < jobs/02_job32_hive.hql
-- Per AWS EMR, sostituire BASE_PATH con s3://tuo-bucket

-- Aumento della memoria per i task MapReduce locali per evitare l'errore "return code 2"
SET mapreduce.map.memory.mb=2048;
SET mapreduce.map.java.opts=-Xmx1638m;
SET mapreduce.reduce.memory.mb=2048;
SET mapreduce.reduce.java.opts=-Xmx1638m;
SET hive.exec.reducers.bytes.per.reducer=134217728;

CREATE DATABASE IF NOT EXISTS airline_db;
USE airline_db;

-- ─── 1. Tabella sorgente (Punta a HDFS o S3) ──────────────────────────────
DROP TABLE IF EXISTS flights_raw32_${hivevar:SUBSET};
CREATE EXTERNAL TABLE flights_raw32_${hivevar:SUBSET} (
    ORIGIN              STRING,
    DEP_DELAY           DOUBLE,
    ARR_DELAY           DOUBLE,
    CANCELLED           DOUBLE,
    CANCELLATION_CODE   STRING,
    CARRIER_DELAY       DOUBLE,
    WEATHER_DELAY       DOUBLE,
    NAS_DELAY           DOUBLE,
    SECURITY_DELAY      DOUBLE,
    LATE_AIRCRAFT_DELAY DOUBLE
)
PARTITIONED BY (YEAR INT, MONTH INT)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}${hivevar:INPUT_DIR}';

-- ─── 2. Aggiunta partizioni YEAR+MONTH ────────────────────────────────────
ALTER TABLE flights_raw32_${hivevar:SUBSET} ADD IF NOT EXISTS
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

-- Verifica caricamento record
SELECT COUNT(*) AS totale_record FROM flights_raw32_${hivevar:SUBSET};

-- ─── 3. Tabella risultato (Esterna, salva su HDFS o S3 stabilmente) ──────
DROP TABLE IF EXISTS delay_report_${hivevar:SUBSET};
CREATE EXTERNAL TABLE delay_report_${hivevar:SUBSET} (
    airport         STRING,
    month           INT,
    avg_arr_delay   DOUBLE,
    avg_dep_delay   DOUBLE,
    delay_band      STRING,
    flight_count    BIGINT,
    top3_causes     STRING
)
STORED AS PARQUET
LOCATION '${hivevar:BASE_PATH}/data/outputs/job32/hive_${hivevar:SUBSET}';

-- ─── 4. Query principale ─────────────────────────────────────────────────
-- La query calcola:
--   A) Statistiche per fascia di ritardo (basso/medio/alto) per (aeroporto, mese)
--   B) Top 3 cause di ritardo/cancellazione per (aeroporto, mese)
--   C) Join tra A e B con formato delle cause come stringa
WITH stats AS (
    SELECT
        ORIGIN                                                    AS airport,
        COALESCE(MONTH, 0)                                        AS month,
        CASE
            WHEN DEP_DELAY < 15 OR DEP_DELAY IS NULL THEN 'basso'
            WHEN DEP_DELAY <= 60                      THEN 'medio'
            ELSE                                           'alto'
        END                                                       AS delay_band,
        COUNT(*)                                                  AS flight_count,
        ROUND(AVG(COALESCE(DEP_DELAY, 0.0)), 2)                  AS avg_dep_delay,
        ROUND(AVG(COALESCE(ARR_DELAY, 0.0)), 2)                  AS avg_arr_delay
    FROM flights_raw32_${hivevar:SUBSET}
    GROUP BY
        ORIGIN,
        COALESCE(MONTH, 0),
        CASE
            WHEN DEP_DELAY < 15 OR DEP_DELAY IS NULL THEN 'basso'
            WHEN DEP_DELAY <= 60                      THEN 'medio'
            ELSE                                           'alto'
        END
),
causes_agg AS (
    SELECT
        ORIGIN                                                                                                          AS airport,
        COALESCE(MONTH, 0)                                                                                              AS month,
        SUM(CASE WHEN CANCELLED = 1 AND CANCELLATION_CODE = 'A' THEN 1.0 ELSE 0.0 END)
            + SUM(COALESCE(CARRIER_DELAY, 0.0))      AS carrier_val,
        SUM(CASE WHEN CANCELLED = 1 AND CANCELLATION_CODE = 'B' THEN 1.0 ELSE 0.0 END)
            + SUM(COALESCE(WEATHER_DELAY, 0.0))      AS weather_val,
        SUM(CASE WHEN CANCELLED = 1 AND CANCELLATION_CODE = 'C' THEN 1.0 ELSE 0.0 END)
            + SUM(COALESCE(NAS_DELAY, 0.0))          AS nas_val,
        SUM(CASE WHEN CANCELLED = 1 AND CANCELLATION_CODE = 'D' THEN 1.0 ELSE 0.0 END)
            + SUM(COALESCE(SECURITY_DELAY, 0.0))     AS security_val,
        SUM(COALESCE(LATE_AIRCRAFT_DELAY, 0.0))      AS late_aircraft_val
    FROM flights_raw32_${hivevar:SUBSET}
    GROUP BY ORIGIN, COALESCE(MONTH, 0)
),
causes_ranked AS (
    -- Costruiamo array di (valore, nome) e prendiamo il TOP 3 con una tecnica pivot
    -- Selezioniamo top3 ordinando manualmente le 5 cause con CASE/GREATEST
    SELECT
        airport,
        month,
        carrier_val,
        weather_val,
        nas_val,
        security_val,
        late_aircraft_val,
        -- Rank delle 5 cause per valore decrescente
        RANK() OVER (PARTITION BY airport, month ORDER BY carrier_val     DESC) AS rank_carrier,
        RANK() OVER (PARTITION BY airport, month ORDER BY weather_val     DESC) AS rank_weather,
        RANK() OVER (PARTITION BY airport, month ORDER BY nas_val         DESC) AS rank_nas,
        RANK() OVER (PARTITION BY airport, month ORDER BY security_val    DESC) AS rank_security,
        RANK() OVER (PARTITION BY airport, month ORDER BY late_aircraft_val DESC) AS rank_late
    FROM causes_agg
),
-- Costruiamo le stringhe delle top 3 cause come lista Python per compatibilità con output Spark Core
causes_formatted AS (
    SELECT
        airport,
        month,
        -- Elemento 1 (massimo assoluto)
        CASE
            WHEN rank_carrier = 1 AND carrier_val > 0         THEN CONCAT("('Carrier', ", CAST(carrier_val AS STRING), ')')
            WHEN rank_weather = 1 AND weather_val > 0         THEN CONCAT("('Weather', ", CAST(weather_val AS STRING), ')')
            WHEN rank_nas = 1 AND nas_val > 0                 THEN CONCAT("('NAS', ", CAST(nas_val AS STRING), ')')
            WHEN rank_security = 1 AND security_val > 0       THEN CONCAT("('Security', ", CAST(security_val AS STRING), ')')
            WHEN rank_late = 1 AND late_aircraft_val > 0      THEN CONCAT("('Late Aircraft', ", CAST(late_aircraft_val AS STRING), ')')
            ELSE NULL
        END AS cause1,
        -- Elemento 2 (secondo massimo)
        CASE
            WHEN rank_carrier = 2 AND carrier_val > 0         THEN CONCAT("('Carrier', ", CAST(carrier_val AS STRING), ')')
            WHEN rank_weather = 2 AND weather_val > 0         THEN CONCAT("('Weather', ", CAST(weather_val AS STRING), ')')
            WHEN rank_nas = 2 AND nas_val > 0                 THEN CONCAT("('NAS', ", CAST(nas_val AS STRING), ')')
            WHEN rank_security = 2 AND security_val > 0       THEN CONCAT("('Security', ", CAST(security_val AS STRING), ')')
            WHEN rank_late = 2 AND late_aircraft_val > 0      THEN CONCAT("('Late Aircraft', ", CAST(late_aircraft_val AS STRING), ')')
            ELSE NULL
        END AS cause2,
        -- Elemento 3 (terzo massimo)
        CASE
            WHEN rank_carrier = 3 AND carrier_val > 0         THEN CONCAT("('Carrier', ", CAST(carrier_val AS STRING), ')')
            WHEN rank_weather = 3 AND weather_val > 0         THEN CONCAT("('Weather', ", CAST(weather_val AS STRING), ')')
            WHEN rank_nas = 3 AND nas_val > 0                 THEN CONCAT("('NAS', ", CAST(nas_val AS STRING), ')')
            WHEN rank_security = 3 AND security_val > 0       THEN CONCAT("('Security', ", CAST(security_val AS STRING), ')')
            WHEN rank_late = 3 AND late_aircraft_val > 0      THEN CONCAT("('Late Aircraft', ", CAST(late_aircraft_val AS STRING), ')')
            ELSE NULL
        END AS cause3
    FROM causes_ranked
)
INSERT OVERWRITE TABLE delay_report_${hivevar:SUBSET}
SELECT
    s.airport,
    s.month,
    s.avg_arr_delay,
    s.avg_dep_delay,
    s.delay_band,
    s.flight_count,
    CONCAT('[',
        COALESCE(c.cause1, ''),
        CASE WHEN c.cause1 IS NOT NULL AND c.cause2 IS NOT NULL THEN ', ' ELSE '' END,
        COALESCE(c.cause2, ''),
        CASE WHEN c.cause2 IS NOT NULL AND c.cause3 IS NOT NULL THEN ', ' ELSE '' END,
        COALESCE(c.cause3, ''),
    ']')                                                          AS top3_causes
FROM stats s
LEFT JOIN causes_formatted c ON s.airport = c.airport AND s.month = c.month
ORDER BY s.airport, s.month, s.delay_band;

-- ─── 5. Stampa prime 10 righe richieste dal report ───────────────────────
SELECT * FROM delay_report_${hivevar:SUBSET} LIMIT 10;
