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
    .appName(f"Job31-AirlineStatistics-SparkCore-{SUBSET}") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print(f"JOB 3.1 — Statistiche Compagnie Aeree (Spark Core RDD)")
print(f"         Subset: {SUBSET}  |  Input: {INPUT_PATH}")
print("=" * 60)

t_start = time.time()

# 1. Caricamento dati e conversione in RDD di dizionari
df = spark.read.parquet(INPUT_PATH)
record_count = df.count()
print(f"Record caricati: {record_count:,} [subset={SUBSET}]")
rdd = df.rdd.map(lambda row: row.asDict())

# Map step:
# Key: (OP_UNIQUE_CARRIER, ORIGIN)
# Value: (ARR_DELAY, CANCELLED, MONTH)
def map_flight(row):
    airline = row.get("OP_UNIQUE_CARRIER")
    airport = row.get("ORIGIN")
    
    # Cast delay values and cancelled values safely
    arr_delay = row.get("ARR_DELAY")
    arr_delay = float(arr_delay) if arr_delay is not None else None
    
    cancelled = row.get("CANCELLED")
    cancelled = int(cancelled) if cancelled is not None else 0
    
    month = row.get("MONTH")
    month = int(month) if month is not None else None
    
    return ((airline, airport), (arr_delay, cancelled, month))

rdd_mapped = rdd.map(map_flight)

# CombineByKey functions:
# Combiner state structure:
# (count, arr_delay_sum, arr_delay_count, min_arr_delay, max_arr_delay, cancelled_sum, active_months_set)

def create_combiner(val):
    arr_delay, cancelled, month = val
    has_arr = arr_delay is not None
    months = {month} if month is not None else set()
    return (
        1,  # total flights
        arr_delay if has_arr else 0.0,
        1 if has_arr else 0,
        arr_delay if has_arr else float('inf'),
        arr_delay if has_arr else float('-inf'),
        cancelled,
        months
    )

def merge_value(acc, val):
    count, d_sum, d_cnt, d_min, d_max, c_sum, months = acc
    arr_delay, cancelled, month = val
    has_arr = arr_delay is not None
    
    if has_arr:
        d_sum += arr_delay
        d_cnt += 1
        if arr_delay < d_min:
            d_min = arr_delay
        if arr_delay > d_max:
            d_max = arr_delay
            
    if month is not None:
        months.add(month)
        
    return (
        count + 1,
        d_sum,
        d_cnt,
        d_min,
        d_max,
        c_sum + cancelled,
        months
    )

def merge_combiners(acc1, acc2):
    count1, d_sum1, d_cnt1, d_min1, d_max1, c_sum1, months1 = acc1
    count2, d_sum2, d_cnt2, d_min2, d_max2, c_sum2, months2 = acc2
    
    return (
        count1 + count2,
        d_sum1 + d_sum2,
        d_cnt1 + d_cnt2,
        min(d_min1, d_min2),
        max(d_max1, d_max2),
        c_sum1 + c_sum2,
        months1.union(months2)
    )

rdd_aggregated = rdd_mapped.combineByKey(
    create_combiner,
    merge_value,
    merge_combiners
)

# Transform aggregated value into a dict matching the required DataFrame structure
def compute_stats(kv):
    (airline, airport), (count, d_sum, d_cnt, d_min, d_max, c_sum, months) = kv
    
    avg_arr_delay = round(d_sum / d_cnt, 2) if d_cnt > 0 else 0.0
    min_arr_delay = d_min if d_min != float('inf') else 0.0
    max_arr_delay = d_max if d_max != float('-inf') else 0.0
    cancellation_rate_pct = round(c_sum / count * 100, 2) if count > 0 else 0.0
    active_months = sorted(list(months))
    
    return {
        "airline": airline,
        "airport": airport,
        "total_flights": count,
        "min_arr_delay": min_arr_delay,
        "max_arr_delay": max_arr_delay,
        "avg_arr_delay": avg_arr_delay,
        "cancellation_rate_pct": cancellation_rate_pct,
        "active_months": active_months
    }

# Convert back to DataFrame, order by airline and airport, and show
df_final = spark.createDataFrame(rdd_aggregated.map(compute_stats)) \
    .orderBy("airline", "airport")

# Select columns in exact order
df_final = df_final.select(
    "airline", "airport", "total_flights", 
    "min_arr_delay", "max_arr_delay", "avg_arr_delay", 
    "cancellation_rate_pct", "active_months"
)

print("\nPrime 10 righe del risultato:")
df_final.show(10, truncate=False)

# Scrittura output
print(f"Salvataggio JSON in: {OUTPUT_PATH}/json")
df_final.coalesce(1).write.mode("overwrite").json(OUTPUT_PATH + "/json")

print(f"Salvataggio CSV in: {OUTPUT_PATH}/csv")
df_csv = df_final.withColumn("active_months", F.concat_ws(", ", F.col("active_months")))
df_csv.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_PATH + "/csv")

t_end = time.time()
elapsed = t_end - t_start
print(f"\n✅ Job 3.1 completato in {elapsed:.1f}s")

# Benchmarking
timing_data = spark.createDataFrame(
    [("job31", "spark_core", SUBSET, elapsed, record_count)],
    ["job", "technology", "subset", "elapsed_seconds", "record_count"]
)
timing_data.write.mode("append").parquet(TIMINGS_PATH)

spark.stop()
