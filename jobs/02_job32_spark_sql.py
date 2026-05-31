import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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

OUTPUT_BASE  = f"{BASE_PATH}/data/outputs/job32"
OUTPUT_PATH  = f"{OUTPUT_BASE}/{SUBSET}"
TIMINGS_PATH = f"{BASE_PATH}/data/outputs/timings"

spark = SparkSession.builder \
    .appName(f"Job32-DelayReport-SparkSQL-{SUBSET}") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(f"JOB 3.2 — Report Ritardi per Aeroporto (Spark SQL)")
print(f"         Subset: {SUBSET}  |  Input: {INPUT_PATH}")
print("=" * 60)

t_start = time.time()

# 1. Caricamento dati puliti
df = spark.read.parquet(INPUT_PATH)
record_count = df.count()
print(f"Record caricati: {record_count:,} [subset={SUBSET}]")
df.createOrReplaceTempView("flights")

# UDF per formattare le Top 3 cause di ritardo/cancellazione come stringa di tuple Python
@F.udf(returnType=StringType())
def get_top3_causes_udf(carrier_val, weather_val, nas_val, security_val, late_aircraft_val):
    causes = []
    if carrier_val and carrier_val > 0:
        causes.append(("Carrier", float(carrier_val)))
    if weather_val and weather_val > 0:
        causes.append(("Weather", float(weather_val)))
    if nas_val and nas_val > 0:
        causes.append(("NAS", float(nas_val)))
    if security_val and security_val > 0:
        causes.append(("Security", float(security_val)))
    if late_aircraft_val and late_aircraft_val > 0:
        causes.append(("Late Aircraft", float(late_aircraft_val)))
    
    # Ordina per impatto decrescente
    top3 = sorted(causes, key=lambda x: -x[1])[:3]
    return str(top3)

spark.udf.register("get_top3_causes", get_top3_causes_udf)

query = """
WITH stats AS (
    SELECT
        ORIGIN AS airport,
        COALESCE(MONTH, 0) AS month,
        CASE 
            WHEN DEP_DELAY < 15 OR DEP_DELAY IS NULL THEN 'basso'
            WHEN DEP_DELAY <= 60 THEN 'medio'
            ELSE 'alto'
        END AS delay_band,
        COUNT(*) AS flight_count,
        ROUND(AVG(COALESCE(DEP_DELAY, 0.0)), 2) AS avg_dep_delay,
        ROUND(AVG(COALESCE(ARR_DELAY, 0.0)), 2) AS avg_arr_delay
    FROM flights
    GROUP BY ORIGIN, COALESCE(MONTH, 0),
             CASE 
                 WHEN DEP_DELAY < 15 OR DEP_DELAY IS NULL THEN 'basso'
                 WHEN DEP_DELAY <= 60 THEN 'medio'
                 ELSE 'alto'
             END
),
causes_raw AS (
    SELECT
        ORIGIN AS airport,
        COALESCE(MONTH, 0) AS month,
        SUM(CASE WHEN CANCELLED = 1 AND CANCELLATION_CODE = 'A' THEN 1.0 ELSE 0.0 END)
            + SUM(COALESCE(CARRIER_DELAY, 0.0))      AS carrier_val,
        SUM(CASE WHEN CANCELLED = 1 AND CANCELLATION_CODE = 'B' THEN 1.0 ELSE 0.0 END)
            + SUM(COALESCE(WEATHER_DELAY, 0.0))      AS weather_val,
        SUM(CASE WHEN CANCELLED = 1 AND CANCELLATION_CODE = 'C' THEN 1.0 ELSE 0.0 END)
            + SUM(COALESCE(NAS_DELAY, 0.0))          AS nas_val,
        SUM(CASE WHEN CANCELLED = 1 AND CANCELLATION_CODE = 'D' THEN 1.0 ELSE 0.0 END)
            + SUM(COALESCE(SECURITY_DELAY, 0.0))     AS security_val,
        SUM(COALESCE(LATE_AIRCRAFT_DELAY, 0.0))      AS late_aircraft_val
    FROM flights
    GROUP BY ORIGIN, COALESCE(MONTH, 0)
)
SELECT
    s.airport,
    s.month,
    s.avg_arr_delay,
    s.avg_dep_delay,
    s.delay_band,
    s.flight_count,
    get_top3_causes(c.carrier_val, c.weather_val, c.nas_val, c.security_val, c.late_aircraft_val) AS top3_causes
FROM stats s
LEFT JOIN causes_raw c ON s.airport = c.airport AND s.month = c.month
ORDER BY s.airport, s.month, s.delay_band
"""

df_result = spark.sql(query)

# Select columns in exact order
df_result = df_result.select(
    "airport", "month", "avg_arr_delay", "avg_dep_delay", 
    "delay_band", "flight_count", "top3_causes"
)

print("\nPrime 10 righe del risultato:")
df_result.show(10, truncate=False)

# Scrittura output
print(f"Salvataggio JSON in: {OUTPUT_PATH}/json")
df_result.coalesce(1).write.mode("overwrite").json(OUTPUT_PATH + "/json")

print(f"Salvataggio CSV in: {OUTPUT_PATH}/csv")
df_result.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_PATH + "/csv")

t_end = time.time()
elapsed = t_end - t_start
print(f"\n✅ Job 3.2 completato in {elapsed:.1f}s")

# Benchmarking
timing_data = spark.createDataFrame(
    [("job32", "spark_sql", SUBSET, elapsed, record_count)],
    ["job", "technology", "subset", "elapsed_seconds", "record_count"]
)
timing_data.write.mode("append").parquet(TIMINGS_PATH)

spark.stop()
