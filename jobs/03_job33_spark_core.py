import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import Row

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

if SUBSET.startswith("pct_"):
    INPUT_PATH = f"{SUBSETS_BASE}/{SUBSET}"
else:
    INPUT_PATH = CLEANED_PATH
    SUBSET = "full"

OUTPUT_BASE  = f"{BASE_PATH}/data/outputs/job33"
OUTPUT_PATH  = f"{OUTPUT_BASE}/{SUBSET}"
TIMINGS_PATH = f"{BASE_PATH}/data/outputs/timings"

spark = SparkSession.builder \
    .appName(f"Job33-AnomalyRanking-SparkCore-{SUBSET}") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print(f"JOB 3.3 — Ranking Anomalie (Spark Core RDD)")
print(f"         Subset: {SUBSET}  |  Input: {INPUT_PATH}")
print("=" * 60)

t_start = time.time()

# 1. Caricamento dati e conversione in RDD
df = spark.read.parquet(INPUT_PATH)
record_count = df.count()
print(f"Record caricati: {record_count:,} [subset={SUBSET}]")
rdd = df.rdd.map(lambda row: row.asDict())

# RDD cache per efficienza selettiva
rdd.cache()

# ─── 2. Calcolo statistiche per Aeroporto + Compagnia ─────────────────────
def map_airline_airport(row):
    origin = row.get("ORIGIN")
    carrier = row.get("OP_UNIQUE_CARRIER")
    if not origin or not str(origin).strip() or not carrier or not str(carrier).strip():
        return None
    
    dep_delay = row.get("DEP_DELAY")
    arr_delay = row.get("ARR_DELAY")
    cancelled = int(row.get("CANCELLED") or 0)
    
    dep_delay = float(dep_delay) if dep_delay is not None else None
    arr_delay = float(arr_delay) if arr_delay is not None else None
    
    return ((origin, carrier), (1, dep_delay, arr_delay, cancelled))

def create_combiner(val):
    _, dep, arr, canc = val
    has_dep = dep is not None and canc == 0
    has_arr = arr is not None and canc == 0
    return (
        1, 
        dep if has_dep else 0.0, 
        1 if has_dep else 0,
        arr if has_arr else 0.0,
        1 if has_arr else 0,
        canc
    )

def merge_value(state, val):
    fc, d_sum, d_cnt, a_sum, a_cnt, c_sum = state
    _, dep, arr, canc = val
    has_dep = dep is not None and canc == 0
    has_arr = arr is not None and canc == 0
    return (
        fc + 1,
        d_sum + (dep if has_dep else 0.0),
        d_cnt + (1 if has_dep else 0),
        a_sum + (arr if has_arr else 0.0),
        a_cnt + (1 if has_arr else 0),
        c_sum + canc
    )

def merge_combiners(state1, state2):
    return (
        state1[0] + state2[0],
        state1[1] + state2[1],
        state1[2] + state2[2],
        state1[3] + state2[3],
        state1[4] + state2[4],
        state1[5] + state2[5]
    )

rdd_airline_agg = rdd.map(map_airline_airport).filter(lambda x: x is not None) \
    .combineByKey(create_combiner, merge_value, merge_combiners)

def map_airline_stats(kv):
    (airport, airline), (fc, d_sum, d_cnt, a_sum, a_cnt, c_sum) = kv
    avg_dep = round(d_sum / d_cnt, 2) if d_cnt > 0 else 0.0
    avg_arr = round(a_sum / a_cnt, 2) if a_cnt > 0 else 0.0
    cancellation_rate = round(c_sum / fc * 100.0, 2) if fc > 0 else 0.0
    return (airport, {
        "airline": airline,
        "flight_count": fc,
        "avg_dep_delay": avg_dep,
        "avg_arr_delay": avg_arr,
        "cancellation_rate_pct": cancellation_rate
    })

rdd_airline_stats = rdd_airline_agg.map(map_airline_stats)

# ─── 3. Calcolo statistiche globali per Aeroporto ─────────────────────────
def map_airport(row):
    origin = row.get("ORIGIN")
    if not origin or not str(origin).strip():
        return None
    
    dep_delay = row.get("DEP_DELAY")
    cancelled = int(row.get("CANCELLED") or 0)
    dep_delay = float(dep_delay) if dep_delay is not None else None
    
    has_dep = dep_delay is not None and cancelled == 0
    return (origin, (1, dep_delay if has_dep else 0.0, 1 if has_dep else 0))

def create_combiner_ap(val):
    return val

def merge_value_ap(state, val):
    return (state[0] + val[0], state[1] + val[1], state[2] + val[2])

def merge_combiners_ap(state1, state2):
    return (state1[0] + state2[0], state1[1] + state2[1], state1[2] + state2[2])

rdd_airport_agg = rdd.map(map_airport).filter(lambda x: x is not None) \
    .combineByKey(create_combiner_ap, merge_value_ap, merge_combiners_ap)

def map_airport_stats(kv):
    airport, (fc, d_sum, d_cnt) = kv
    avg_dep = round(d_sum / d_cnt, 2) if d_cnt > 0 else 0.0
    return (airport, {
        "airport_total_flights": fc,
        "airport_avg_dep_delay": avg_dep
    })

rdd_airport_stats = rdd_airport_agg.map(map_airport_stats)

# ─── 4. Join e Calcolo Rank (Grouping per Aeroporto in memoria) ───────────
joined = rdd_airline_stats.join(rdd_airport_stats)

def map_joined(kv):
    airport, (airline_info, airport_info) = kv
    
    dep_delay_diff = round(airline_info["avg_dep_delay"] - airport_info["airport_avg_dep_delay"], 2)
    
    if dep_delay_diff > 15.0:
        anomaly = "ANOMALO_PEGGIORE"
    elif dep_delay_diff < -15.0:
        anomaly = "ANOMALO_MIGLIORE"
    else:
        anomaly = "NELLA_NORMA"
        
    res = {
        "airport": airport,
        "airline": airline_info["airline"],
        "flight_count": airline_info["flight_count"],
        "avg_dep_delay": airline_info["avg_dep_delay"],
        "avg_arr_delay": airline_info["avg_arr_delay"],
        "cancellation_rate_pct": airline_info["cancellation_rate_pct"],
        "airport_avg_dep_delay": airport_info["airport_avg_dep_delay"],
        "airport_total_flights": airport_info["airport_total_flights"],
        "dep_delay_diff": dep_delay_diff,
        "anomaly_label": anomaly
    }
    return (airport, res)

def rank_airlines(kv):
    airport, airlines_iter = kv
    airlines = list(airlines_iter)
    
    # Ordinamento per rank_in_airport (ORDER BY avg_dep_delay ASC)
    airlines.sort(key=lambda x: x["avg_dep_delay"])
    
    results = []
    current_rank = 1
    for i, a in enumerate(airlines):
        if i > 0 and a["avg_dep_delay"] != airlines[i-1]["avg_dep_delay"]:
            current_rank = i + 1
        a["rank_in_airport"] = current_rank
        results.append(a)
        
    return results

rdd_ranked = joined.map(map_joined).groupByKey().flatMap(rank_airlines)

# ─── 5. Conversione DataFrame e output ─────────────────────────────────────
df_final = spark.createDataFrame(rdd_ranked).orderBy("airport", "rank_in_airport")

df_final = df_final.select(
    "airport", "airline", "flight_count", 
    "avg_dep_delay", "avg_arr_delay", "cancellation_rate_pct", 
    "airport_avg_dep_delay", "airport_total_flights", 
    "dep_delay_diff", "rank_in_airport", "anomaly_label"
)

print("\nPrime 10 righe del risultato:")
df_final.show(10, truncate=False)

print("\nDistribuzione label anomalie:")
df_final.groupBy("anomaly_label").count().orderBy(F.desc("count")).show()

print("\nTop 10 compagnie più anomale (peggiori):")
df_final.filter(F.col("anomaly_label") == "ANOMALO_PEGGIORE") \
    .orderBy(F.desc("dep_delay_diff")) \
    .select("airline", "airport", "avg_dep_delay", "dep_delay_diff", "rank_in_airport") \
    .show(10, truncate=False)

print(f"Salvataggio JSON → {OUTPUT_PATH}/json")
df_final.coalesce(1).write.mode("overwrite").json(OUTPUT_PATH + "/json")

print(f"Salvataggio CSV  → {OUTPUT_PATH}/csv")
df_final.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_PATH + "/csv")

t_end = time.time()
elapsed = t_end - t_start

print(f"\n✅ Job 3.3 completato in {elapsed:.1f}s  [subset={SUBSET}, records={record_count:,}]")
print(f"   Output: {OUTPUT_PATH}")

timing_data = spark.createDataFrame(
    [("job33", "spark_core", SUBSET, elapsed, record_count)],
    ["job", "technology", "subset", "elapsed_seconds", "record_count"]
)
timing_data.write.mode("append").parquet(TIMINGS_PATH)

spark.stop()
