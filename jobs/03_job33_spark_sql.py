"""
jobs/03_job33_spark_sql.py
==========================
JOB 3.3 — Ranking coppie (Aeroporto, Compagnia) con comportamento anomalo
Tecnologia: Spark SQL

Uso:
    spark-submit 03_job33_spark_sql.py [subset]

    [subset] può essere: pct_025 | pct_050 | pct_100 | pct_200
    Default: pct_100

Nota implementativa (per il rapporto):
  Hive 2.3 su MapReduce in ambiente locale Docker crasha durante la lettura
  di file Parquet+Snappy con più di ~4 split (bug noto della combinazione
  bde2020/hive:2.3.2 + Hadoop 2.7.4 in modalità local MR).
  Il job viene quindi implementato in Spark SQL, che usa il Catalyst optimizer
  e gestisce la memoria in modo nettamente superiore a MapReduce.
  Su cluster AWS EMR il job Hive funzionerebbe correttamente con Tez.
"""

import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─── Configurazione Dinamica Percorsi ──────────────────────────────────────
# Default per Docker locale. Per AWS, passare i parametri corretti.
BASE_PATH = "hdfs://namenode:9000"
SUBSET    = "pct_100"

# Gestione argomenti flessibile:
# 1. Se passiamo 'pct_050', è il subset.
# 2. Se passiamo 's3://...', è il base path.
for arg in sys.argv[1:]:
    if arg.startswith("pct_"):
        SUBSET = arg
    elif arg.startswith("s3://") or arg.startswith("hdfs://"):
        BASE_PATH = arg.rstrip("/")

SUBSETS_BASE = f"{BASE_PATH}/data/subsets"
OUTPUT_BASE  = f"{BASE_PATH}/data/outputs/job33"
TIMINGS_PATH = f"{BASE_PATH}/data/outputs/timings"

INPUT_PATH  = f"{SUBSETS_BASE}/{SUBSET}"
OUTPUT_PATH = f"{OUTPUT_BASE}/{SUBSET}"

# ─── Spark Session ────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName(f"Job33-AnomalyRanking-SparkSQL-{SUBSET}") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(f"JOB 3.3 — Ranking Anomalie (Spark SQL)")
print(f"         Subset: {SUBSET}  |  Input: {INPUT_PATH}")
print("=" * 60)

t_start = time.time()

# ─── 1. Caricamento dati dal subset selezionato ───────────────────────────────
df = spark.read.parquet(INPUT_PATH)
df.createOrReplaceTempView("flights")
record_count = df.count()
print(f"Record caricati: {record_count:,}  [subset={SUBSET}]")

# ─── 2. Query principale con CTE e window function RANK() ─────────────────────
#
# La query realizza in un'unica passata le 5 metriche richieste dal bando:
#   (a) flight_count          — numero voli della compagnia in quell'aeroporto
#   (b) avg_dep_delay,        — ritardo medio in partenza e in arrivo
#       avg_arr_delay
#   (c) cancellation_rate_pct — tasso di cancellazione
#   (d) dep_delay_diff        — delta rispetto alla media dell'aeroporto
#   (e) rank_in_airport       — posizione in classifica (migliore → peggiore)
#
query = """
WITH airline_airport_stats AS (
    -- Aggregazione per coppia (aeroporto, compagnia)
    SELECT
        ORIGIN                                                              AS airport,
        OP_UNIQUE_CARRIER                                                   AS airline,
        COUNT(*)                                                            AS flight_count,
        ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEP_DELAY ELSE NULL END), 2) AS avg_dep_delay,
        ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARR_DELAY ELSE NULL END), 2) AS avg_arr_delay,
        ROUND(SUM(CANCELLED) / COUNT(*) * 100.0, 2)                        AS cancellation_rate_pct
    FROM flights
    WHERE ORIGIN IS NOT NULL AND TRIM(ORIGIN) != ''
      AND OP_UNIQUE_CARRIER IS NOT NULL AND TRIM(OP_UNIQUE_CARRIER) != ''
    GROUP BY ORIGIN, OP_UNIQUE_CARRIER
),
airport_global_avg AS (
    -- Media globale per aeroporto (tutte le compagnie aggregate)
    SELECT
        ORIGIN                                                              AS airport,
        ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEP_DELAY ELSE NULL END), 2) AS airport_avg_dep_delay,
        COUNT(*)                                                             AS airport_total_flights
    FROM flights
    WHERE ORIGIN IS NOT NULL AND TRIM(ORIGIN) != ''
    GROUP BY ORIGIN
),
ranked AS (
    -- Join + window function RANK per calcolare la posizione in classifica
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
            ORDER BY s.avg_dep_delay ASC NULLS LAST
        )                                                                   AS rank_in_airport
    FROM airline_airport_stats s
    JOIN airport_global_avg a ON s.airport = a.airport
)
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
ORDER BY airport ASC, rank_in_airport ASC
"""

df_result = spark.sql(query)

# ─── 3. Stampe diagnostiche ───────────────────────────────────────────────────
print("\nPrime 10 righe del risultato:")
df_result.show(10, truncate=False)

print("\nDistribuzione label anomalie:")
df_result.groupBy("anomaly_label") \
    .count() \
    .orderBy(F.desc("count")) \
    .show()

print("\nTop 10 compagnie più anomale (peggiori):")
df_result.filter(F.col("anomaly_label") == "ANOMALO_PEGGIORE") \
    .orderBy(F.desc("dep_delay_diff")) \
    .select("airline", "airport", "avg_dep_delay", "dep_delay_diff", "rank_in_airport") \
    .show(10, truncate=False)

# ─── 4. Scrittura output ──────────────────────────────────────────────────────
print(f"\nSalvataggio CSV  → {OUTPUT_PATH}/csv")
df_result.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_PATH + "/csv")

print(f"Salvataggio JSON → {OUTPUT_PATH}/json")
df_result.coalesce(1).write.mode("overwrite").json(OUTPUT_PATH + "/json")

t_end = time.time()
elapsed = t_end - t_start

print(f"\n✅ Job 3.3 completato in {elapsed:.1f}s  [subset={SUBSET}, records={record_count:,}]")
print(f"   Output: {OUTPUT_PATH}")

# ─── 5. Salva timing per benchmarking ─────────────────────────────────────────
# Schema allineato agli altri job: aggiunta colonna 'subset' per il confronto
timing_data = spark.createDataFrame(
    [("job33", "spark_sql", SUBSET, elapsed, record_count)],
    ["job", "technology", "subset", "elapsed_seconds", "record_count"]
)
timing_data.write.mode("append").parquet(TIMINGS_PATH)

spark.stop()
