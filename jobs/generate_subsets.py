import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─── Configurazione Dinamica Percorsi ──────────────────────────────────────
BASE_PATH = "hdfs://namenode:9000"
for arg in sys.argv[1:]:
    if arg.startswith("s3://") or arg.startswith("hdfs://"):
        BASE_PATH = arg.rstrip("/")

CLEANED = f"{BASE_PATH}/data/cleaned"
BASE_OUT = f"{BASE_PATH}/data/subsets"

spark = SparkSession.builder \
    .appName("GenerateSubsets") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

df_full = spark.read.parquet(CLEANED)

# Generazione subset 25%, 50%, 100%
for pct in [0.25, 0.50, 1.00]:
    label = f"pct_{int(pct*100):03d}"
    out = f"{BASE_OUT}/{label}"
    
    sample = df_full.sample(fraction=pct, seed=42) if pct < 1.0 else df_full
    
    # Manteniamo il partizionamento fisico YEAR/MONTH per Hive
    sample.write.mode("overwrite") \
          .partitionBy("YEAR", "MONTH") \
          .parquet(out)
    print(f"Generato subset {label}")

# Generazione subset 200% (Replica controllata come da bando)
df_200 = df_full.unionAll(df_full)
df_200.write.mode("overwrite") \
      .partitionBy("YEAR", "MONTH") \
      .parquet(f"{BASE_OUT}/pct_200")
print("Generato subset pct_200")

spark.stop()