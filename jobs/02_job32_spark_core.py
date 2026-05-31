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

OUTPUT_BASE  = f"{BASE_PATH}/data/outputs/job32"
OUTPUT_PATH  = f"{OUTPUT_BASE}/{SUBSET}"
TIMINGS_PATH = f"{BASE_PATH}/data/outputs/timings"

spark = SparkSession.builder \
    .appName(f"Job32-DelayReport-SparkCore-{SUBSET}") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print(f"JOB 3.2 — Report Ritardi per Aeroporto (Spark Core RDD)")
print(f"         Subset: {SUBSET}  |  Input: {INPUT_PATH}")
print("=" * 60)

t_start = time.time()

# 1. Caricamento dati e conversione in RDD di dizionari
df = spark.read.parquet(INPUT_PATH)
print(f"Record caricati: {df.count():,}")
rdd = df.rdd.map(lambda row: row.asDict())

# --- Funzione di classificazione fascia ritardo ---
def get_delay_band(dep_delay):
    if dep_delay is None: return "basso"
    d = float(dep_delay)
    if d < 15: return "basso"
    elif d <= 60: return "medio"
    else: return "alto"

# --- STEP A: Conteggi e medie per (aeroporto, mese, fascia) ---
def map_delay_band(row):
    band = get_delay_band(row.get("DEP_DELAY"))
    key  = (row["ORIGIN"], int(row["MONTH"]) if row["MONTH"] else 0, band)
    dep  = float(row["DEP_DELAY"]) if row["DEP_DELAY"] is not None else 0.0
    arr  = float(row["ARR_DELAY"]) if row["ARR_DELAY"] is not None else 0.0
    return (key, (dep, arr, 1))

rdd_mapped = rdd.map(map_delay_band)

# Utilizzo di combineByKey per massima efficienza (aggregazione locale pre-shuffle)
rdd_aggregated = rdd_mapped.combineByKey(
    lambda val: (val[0], val[1], val[2]),
    lambda acc, val: (acc[0] + val[0], acc[1] + val[1], acc[2] + val[2]),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1], acc1[2] + acc2[2])
)

def compute_averages(kv):
    (airport, month, band), (sum_dep, sum_arr, count) = kv
    return {
        "airport": airport, "month": month, "delay_band": band,
        "flight_count": count,
        "avg_dep_delay": round(sum_dep / count, 2) if count > 0 else 0.0,
        "avg_arr_delay": round(sum_arr / count, 2) if count > 0 else 0.0,
    }

rdd_stats = rdd_aggregated.map(compute_averages)

# --- STEP B: Cause di ritardo/cancellazione ---
DELAY_LABELS = {"CARRIER_DELAY": "Carrier", "WEATHER_DELAY": "Weather", "NAS_DELAY": "NAS", "SECURITY_DELAY": "Security", "LATE_AIRCRAFT_DELAY": "Late Aircraft"}
CANCEL_LABELS = {"A": "Carrier", "B": "Weather", "C": "NAS", "D": "Security"}

def map_causes(row):
    key = (row["ORIGIN"], int(row["MONTH"]) if row["MONTH"] else 0)
    causes = {}
    cc = row.get("CANCELLATION_CODE")
    if cc and row.get("CANCELLED") == 1:
        label = CANCEL_LABELS.get(cc, cc)
        causes[label] = causes.get(label, 0) + 1
    for col, label in DELAY_LABELS.items():
        val = row.get(col)
        if val and float(val) > 0:
            causes[label] = causes.get(label, 0) + float(val)
    return (key, causes)

def merge_causes(d1, d2):
    merged = dict(d1)
    for k, v in d2.items(): merged[k] = merged.get(k, 0) + v
    return merged

rdd_causes = rdd.map(map_causes).reduceByKey(merge_causes).map(lambda kv: {
    "airport": kv[0][0], "month": kv[0][1],
    "top3_causes": str(sorted(kv[1].items(), key=lambda x: -x[1])[:3])
})

# --- STEP C: Join e Output ---
df_stats = spark.createDataFrame(rdd_stats)
df_causes = spark.createDataFrame(rdd_causes)
df_final = df_stats.join(df_causes, on=["airport", "month"], how="left").orderBy("airport", "month", "delay_band")

df_final.show(10, truncate=False)
df_final.coalesce(1).write.mode("overwrite").json(OUTPUT_PATH + "/json")
df_final.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_PATH + "/csv")

t_end = time.time()
elapsed = t_end - t_start
print(f"\n✅ Job 3.2 completato in {elapsed:.1f}s")

# Salvataggio tempi per benchmarking
timing_data = spark.createDataFrame(
    [("job32", "spark_core", SUBSET, elapsed, df.count())],
    ["job", "technology", "subset", "elapsed_seconds", "record_count"]
)
timing_data.write.mode("append").parquet(TIMINGS_PATH)

spark.stop()