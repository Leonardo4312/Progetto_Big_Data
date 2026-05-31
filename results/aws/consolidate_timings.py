from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ConsolidateTimings").getOrCreate()

# Leggi tutti i file parquet nella cartella timings
timings_path = "s3://bigdata-flight2024-ziappo/data/outputs/timings"
df_timings = spark.read.parquet(timings_path)

# Ordina per job e subset per avere una tabella pulita
df_final = df_timings.orderBy("job", "subset")

# Mostra a video per controllo
df_final.show(50)

# Salva un unico file CSV ordinato
output_path = "s3://bigdata-flight2024-ziappo/data/outputs/final_report_timings"
df_final.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print(f"✅ Tabella riassuntiva salvata in {output_path}")
spark.stop()