import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─── Configurazione Dinamica Percorsi ──────────────────────────────────────
# Default per Docker locale. Per AWS, passare i parametri corretti.
BASE_PATH = "hdfs://namenode:9000"
SUBSET    = "full"

# Gestione argomenti flessibile:
# 1. Se passiamo 'pct_050', è il subset.
# 2. Se passiamo 's3://...', è il base path.
for arg in sys.argv[1:]:
    if arg.startswith("pct_"):
        SUBSET = arg
    elif arg.startswith("s3://") or arg.startswith("hdfs://"):
        BASE_PATH = arg.rstrip("/")

SUBSETS_BASE = f"{BASE_PATH}/data/subsets"
CLEANED_PATH = f"{BASE_PATH}/data/cleaned"

# Se è specificato un subset (es. pct_050), leggiamo da lì
if SUBSET.startswith("pct_"):
    INPUT_PATH = f"{SUBSETS_BASE}/{SUBSET}"
else:
    INPUT_PATH = CLEANED_PATH
    SUBSET = "full"

OUTPUT_BASE  = f"{BASE_PATH}/data/outputs/job31"
OUTPUT_PATH  = f"{OUTPUT_BASE}/{SUBSET}"
TIMINGS_PATH = f"{BASE_PATH}/data/outputs/timings"

spark = SparkSession.builder \
    .appName(f"Job31-AirlineStatistics-SparkSQL-{SUBSET}") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Impostiamo il log a WARN per evitare troppe scritte in console
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(f"JOB 3.1 — Statistiche Compagnie Aeree (Spark SQL)")
print(f"         Subset: {SUBSET}  |  Input: {INPUT_PATH}")
print("=" * 60)

t_start = time.time()

# ─── 1. Caricamento dati puliti ───────────────────────────────────────────────
# Spark riconosce automaticamente lo schema dal formato Parquet
df = spark.read.parquet(INPUT_PATH)
df.createOrReplaceTempView("flights")

record_count = df.count()
print(f"Record caricati correttamente: {record_count:,} [subset={SUBSET}]")

# ─── 2. Query principale ──────────────────────────────────────────────────────
# Utilizziamo OP_UNIQUE_CARRIER come identificato durante il cleaning
query = """
SELECT
    OP_UNIQUE_CARRIER                           AS airline,
    ORIGIN                                      AS airport,
    COUNT(*)                                    AS total_flights,
    MIN(ARR_DELAY)                              AS min_arr_delay,
    MAX(ARR_DELAY)                              AS max_arr_delay,
    ROUND(AVG(ARR_DELAY), 2)                    AS avg_arr_delay,
    ROUND(SUM(CANCELLED) / COUNT(*) * 100, 2)   AS cancellation_rate_pct,
    sort_array(collect_set(MONTH))              AS active_months
FROM flights
GROUP BY OP_UNIQUE_CARRIER, ORIGIN
ORDER BY airline, airport
"""

df_result = spark.sql(query)

# ─── 3. Stampa anteprima ───────────────────────────────────────────────────
print("\nPrime 10 righe del risultato:")
df_result.show(10, truncate=False)

# ─── 4. Scrittura output ──────────────────────────────────────────────────────

# A. Salvataggio in JSON (Supporta i tipi Array nativamente)
print(f"Salvataggio JSON in: {OUTPUT_PATH}/json")
df_result.coalesce(1).write.mode("overwrite").json(OUTPUT_PATH + "/json")

# B. Salvataggio in CSV (Richiede la conversione dell'Array in Stringa)
# Trasformiamo [1, 2, 3] in "1, 2, 3" per compatibilità con il formato CSV
print(f"Salvataggio CSV in: {OUTPUT_PATH}/csv")
df_csv = df_result.withColumn("active_months", F.concat_ws(", ", F.col("active_months")))
df_csv.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_PATH + "/csv")

t_end = time.time()
elapsed = t_end - t_start

print(f"\n✅ Job 3.1 completato in {elapsed:.1f}s")

# ─── 5. Benchmarking ───────────────────────────────────────────────────────
# Salviamo i tempi per il confronto finale nel rapporto (Job 3.1 vs Job 3.2)
timing_data = spark.createDataFrame(
    [("job31", "spark_sql", SUBSET, elapsed, record_count)],
    ["job", "technology", "subset", "elapsed_seconds", "record_count"]
)
timing_data.write.mode("append").parquet(TIMINGS_PATH)

spark.stop()