"""
jobs/00_data_cleaning.py
========================
Script di preparazione e pulizia del dataset Flight Delay 2024.
Tecnologia: PySpark (DataFrame API)

Motivazioni delle operazioni (per il rapporto finale):
  1. Selezione colonne: riduce l'I/O e la memoria; le 35 colonne originali
     contengono molti attributi irrilevanti per le analisi 3.1-3.3.
  2. Cast dei tipi: i CSV importano tutto come StringType; i calcoli
     aritmetici sui ritardi richiedono tipi numerici.
  3. Filtraggio NaN su chiavi: record senza compagnia o aeroporto sono
     inutilizzabili in qualsiasi aggregazione.
  4. Filtraggio valori impossibili: ritardi < -300 min o > 1500 min
     sono quasi certamente errori di acquisizione dati.
  5. Normalizzazione CANCELLED: alcune versioni del dataset usano 0/1,
     altre "true"/"false"; standardizziamo a intero 0/1.
  6. Aggiunta colonna MONTH: necessaria per le analisi temporali 3.1/3.2.
  7. Persistenza Parquet + partizionamento: Parquet è ottimale per
     query analitiche (colonnare, compresso); il partizionamento per mese
     consente pruning automatico nei job successivi.
"""

import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, DateType

# ─── Configurazione Dinamica Percorsi ──────────────────────────────────────
# Default per HDFS locale (Docker).
BASE_PATH = "hdfs://namenode:9000"

# Se passiamo un percorso S3 o HDFS come argomento, sovrascriviamo il default
for arg in sys.argv[1:]:
    if arg.startswith("s3://") or arg.startswith("hdfs://"):
        BASE_PATH = arg.rstrip("/")

INPUT_PATH   = f"{BASE_PATH}/user/bigdata/flight2024/raw/flights_2024.csv"
OUTPUT_PATH  = f"{BASE_PATH}/data/cleaned"
LOCAL_INPUT  = "./data/flights_2024.csv"   # fallback locale

# Colonne rilevanti estratte dal dataset Kaggle (35 colonne totali)
# Manteniamo 14 colonne essenziali per le tre analisi.
SELECTED_COLS = [
    "FL_DATE",          # Data del volo (YYYY-MM-DD)
    "OP_UNIQUE_CARRIER",       # Codice IATA compagnia (es. "AA", "DL")
    "OP_CARRIER_FL_NUM",# Numero volo
    "ORIGIN",           # Aeroporto di partenza (IATA)
    "DEST",             # Aeroporto di destinazione (IATA)
    "DEP_DELAY",        # Ritardo partenza (minuti, negativo = anticipo)
    "ARR_DELAY",        # Ritardo arrivo (minuti)
    "CANCELLED",        # 1 = cancellato, 0 = operato
    "CANCELLATION_CODE",# Causa cancellazione: A=carrier, B=weather, C=NAS, D=security
    "CARRIER_DELAY",    # Minuti ritardo causa compagnia
    "WEATHER_DELAY",    # Minuti ritardo causa meteo
    "NAS_DELAY",        # Minuti ritardo causa sistema nazionale
    "SECURITY_DELAY",   # Minuti ritardo causa sicurezza
    "LATE_AIRCRAFT_DELAY", # Minuti ritardo causa aeromobile in ritardo
]

# ─── Spark Session ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("FlightData2024-Cleaning") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("JOB: Data Cleaning — Flight Delay Dataset 2024")
print("=" * 60)

t_start = time.time()

# ─── 1. Lettura CSV ────────────────────────────────────────────────────────
print("\n[1/7] Lettura CSV...")
try:
    df_raw = spark.read.csv(INPUT_PATH, header=True, inferSchema=False)
    print(f"     Sorgente: HDFS ({INPUT_PATH})")
except Exception:
    df_raw = spark.read.csv(LOCAL_INPUT, header=True, inferSchema=False)
    print(f"     Sorgente: locale ({LOCAL_INPUT})")

n_raw = df_raw.count()
print(f"     Record totali grezzi: {n_raw:,}")

# ─── 2. Selezione colonne rilevanti ────────────────────────────────────────
print("\n[2/7] Selezione colonne rilevanti...")
# Alcune versioni del CSV hanno nomi con spazi o maiuscole diverse
available = [c.upper().strip() for c in df_raw.columns]
df_raw = df_raw.toDF(*[c.upper().strip() for c in df_raw.columns])

cols_present = [c for c in SELECTED_COLS if c in df_raw.columns]
cols_missing  = [c for c in SELECTED_COLS if c not in df_raw.columns]
if cols_missing:
    print(f"     ⚠ Colonne non trovate (ignorate): {cols_missing}")

df = df_raw.select(*cols_present)

# ─── 3. Cast dei tipi ──────────────────────────────────────────────────────
print("\n[3/7] Cast dei tipi numerici...")
numeric_cols = [
    "DEP_DELAY", "ARR_DELAY", "CANCELLED",
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY",
]
for col in numeric_cols:
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast(FloatType()))

# CANCELLED deve essere intero 0/1
if "CANCELLED" in df.columns:
    df = df.withColumn(
        "CANCELLED",
        F.when(F.col("CANCELLED").isin("1", "1.0", "true", "True"), 1).otherwise(0).cast(IntegerType())
    )

# ─── 4. Filtraggio record con chiavi mancanti ──────────────────────────────
print("\n[4/7] Rimozione record con chiavi mancanti (ORIGIN, OP_CARRIER)...")
df = df.filter(
    F.col("ORIGIN").isNotNull() &
    F.col("OP_UNIQUE_CARRIER").isNotNull() &
    (F.trim(F.col("ORIGIN")) != "") &
    (F.trim(F.col("OP_UNIQUE_CARRIER")) != "")
)
n_after_nulls = df.count()
print(f"     Record rimasti: {n_after_nulls:,}  (rimossi: {n_raw - n_after_nulls:,})")

# ─── 5. Filtraggio valori impossibili sui ritardi ──────────────────────────
print("\n[5/7] Filtraggio valori anomali sui ritardi...")
# Per i voli cancellati, DEP_DELAY e ARR_DELAY possono essere null → accettabile.
# Per i voli operati, filtriamo range impossibili.
df = df.filter(
    # Volo cancellato → ritardi possono essere null
    (F.col("CANCELLED") == 1) |
    (
        # Volo operato → ritardi in range plausibile [-120, +1500] minuti
        F.col("DEP_DELAY").isNull() | F.col("DEP_DELAY").between(-120, 1500)
    ) & (
        F.col("ARR_DELAY").isNull() | F.col("ARR_DELAY").between(-120, 1500)
    )
)
n_after_range = df.count()
print(f"     Record rimasti: {n_after_range:,}  (rimossi: {n_after_nulls - n_after_range:,})")

# ─── 6. Normalizzazione e colonne derivate ─────────────────────────────────
print("\n[6/7] Aggiunta colonne derivate...")

# Estrai MONTH e YEAR da FL_DATE (formato atteso: YYYY-MM-DD)
df = df.withColumn("FL_DATE", F.to_date(F.col("FL_DATE"), "yyyy-MM-dd"))
df = df.withColumn("MONTH",   F.month(F.col("FL_DATE")))
df = df.withColumn("YEAR",    F.year(F.col("FL_DATE")))

# Normalizza CANCELLATION_CODE: strip + upper
if "CANCELLATION_CODE" in df.columns:
    df = df.withColumn(
        "CANCELLATION_CODE",
        F.when(F.col("CANCELLATION_CODE").isNull(), "N/A")
         .otherwise(F.upper(F.trim(F.col("CANCELLATION_CODE"))))
    )

# Tratta i ritardi NaN dei voli operati come 0
# (volo operato, nessun ritardo registrato = puntuale)
df = df.withColumn(
    "DEP_DELAY",
    F.when((F.col("CANCELLED") == 0) & F.col("DEP_DELAY").isNull(), 0.0)
     .otherwise(F.col("DEP_DELAY"))
)
df = df.withColumn(
    "ARR_DELAY",
    F.when((F.col("CANCELLED") == 0) & F.col("ARR_DELAY").isNull(), 0.0)
     .otherwise(F.col("ARR_DELAY"))
)

# ─── 7. Scrittura Parquet partizionato ─────────────────────────────────────
print("\n[7/7] Scrittura output Parquet partizionato per YEAR, MONTH...")
df.repartition("MONTH") \
  .write \
  .mode("overwrite") \
  .partitionBy("YEAR", "MONTH") \
  .parquet(OUTPUT_PATH)

t_end = time.time()

print("\n" + "=" * 60)
print(f"✅ Cleaning completato in {t_end - t_start:.1f}s")
print(f"   Record finali puliti: {n_after_range:,}")
print(f"   Output: {OUTPUT_PATH}")
print("=" * 60)

spark.stop()
