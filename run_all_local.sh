#!/bin/bash
# run_all_local.sh — Esegui tutti i job con TUTTE le tecnologie in sequenza, misurando i tempi
#
# Uso:
#   bash run_all_local.sh [subset]
#
# [subset] può essere:
#   pct_025 (o 25)  → 25% del dataset
#   pct_050 (o 50)  → 50% del dataset
#   pct_100 (o 100) → dataset completo (default)
#   pct_200 (o 200) → dataset 2x replicato
#   all             → benchmarking su tutti i subset
#
# Esempio:
#   bash run_all_local.sh pct_050   → usa il 50% del dataset
#   bash run_all_local.sh all       → esegue tutti i subset con tutte le tecnologie
#
# Tecnologie eseguite per job:
#   Job 3.1: Spark SQL, Spark Core RDD, Hive
#   Job 3.2: Spark Core RDD, Spark SQL, Hive
#   Job 3.3: Spark Core RDD, Spark SQL, Hive

set -e

# ─── Normalizzazione subset: accetta sia "pct_025" che "25" ──────────────────
raw_subset=${1:-pct_100}
case "$raw_subset" in
  25|pct_25|pct_025)   SUBSET="pct_025" ;;
  50|pct_50|pct_050)   SUBSET="pct_050" ;;
  100|pct_100)         SUBSET="pct_100" ;;
  200|pct_200)         SUBSET="pct_200" ;;
  all)                 SUBSET="all" ;;
  *) echo "[WARN] Subset '${raw_subset}' non riconosciuto. Uso pct_100."; SUBSET="pct_100" ;;
esac

# Percorso completo spark-submit nell'immagine bde2020/spark-master:3.1.1
SPARK_SUBMIT="/spark/bin/spark-submit"

BASE_PATH="hdfs://namenode:9000"

LOGDIR="./logs/local_$(date +%Y%m%d_%H%M%S)"
mkdir -p ${LOGDIR}

TIMING_FILE="./results/timings_local_${SUBSET}.txt"
mkdir -p ./results
echo "job,technology,subset,environment,elapsed_seconds" > ${TIMING_FILE}

# ─── Funzione generica per eseguire un job Spark via spark-submit ─────────────
run_spark_job() {
    local NAME=$1
    local TECH=$2
    local SCRIPT=$3
    local JOB=$4
    local SUBSET_ARG=${5:-""}   # argomento subset opzionale

    echo ""
    echo "═══════════════════════════════════════════════"
    echo "  Eseguendo: ${NAME} (${TECH})  [subset=${SUBSET_ARG:-N/A}]"
    echo "═══════════════════════════════════════════════"

    T_START=$(python3 -c 'import time; print(int(time.time() * 1000))')

    docker exec spark-master ${SPARK_SUBMIT} \
        --master spark://spark-master:7077 \
        --conf spark.app.name="${NAME}" \
        /jobs/${SCRIPT} ${SUBSET_ARG} 2>&1 | tee ${LOGDIR}/${NAME}.log

    T_END=$(python3 -c 'import time; print(int(time.time() * 1000))')
    ELAPSED=$(awk "BEGIN {printf \"%.1f\", ($T_END - $T_START) / 1000}")

    echo "${JOB},${TECH},${SUBSET_ARG:-full},local,${ELAPSED}" >> ${TIMING_FILE}

    # Aggiorna anche il file consolidato se esiste e non siamo già in modalità "all"
    if [ "${SUBSET}" != "all" ] && [ -f "./results/timings_local_all.txt" ]; then
      echo "${JOB},${TECH},${SUBSET_ARG:-full},local,${ELAPSED}" >> "./results/timings_local_all.txt"
    fi

    echo "  ✅ ${NAME}: ${ELAPSED}s"
}

# ─── Funzione per eseguire Hive su un singolo subset ────────────────────────
run_hive_job() {
    local JOB_NAME=$1    # es. Job31
    local JOB_ID=$2      # es. job31
    local HQL_FILE=$3    # es. jobs/01_job31_hive.hql
    local SUB=$4
    local DIR=$5

    echo ""
    echo "═══════════════════════════════════════════════"
    echo "  Eseguendo: ${JOB_NAME} (Hive)  [subset=${SUB}]"
    echo "  Directory sorgente HDFS: ${DIR}"
    echo "═══════════════════════════════════════════════"

    T_START=$(python3 -c 'import time; print(int(time.time() * 1000))')

    docker exec -i hiveserver2 beeline \
        -u jdbc:hive2://localhost:10000 \
        -n hive \
        --hivevar BASE_PATH="${BASE_PATH}" \
        --hivevar SUBSET="${SUB}" \
        --hivevar INPUT_DIR="${DIR}" \
        < ${HQL_FILE} 2>&1 | tee "${LOGDIR}/${JOB_NAME}_hive_${SUB}.log"

    T_END=$(python3 -c 'import time; print(int(time.time() * 1000))')
    ELAPSED=$(awk "BEGIN {printf \"%.1f\", ($T_END - $T_START) / 1000}")

    echo "${JOB_ID},hive,${SUB},local,${ELAPSED}" >> ${TIMING_FILE}

    # Aggiorna anche il file consolidato se esiste e non siamo già in modalità "all"
    if [ "${SUBSET}" != "all" ] && [ -f "./results/timings_local_all.txt" ]; then
      echo "${JOB_ID},hive,${SUB},local,${ELAPSED}" >> "./results/timings_local_all.txt"
    fi

    echo "  ✅ ${JOB_NAME} Hive (${SUB}) completato in: ${ELAPSED}s"
}

# ─── Funzione di orchestrazione completa per un singolo subset ───────────────
run_all_for_subset() {
    local S=$1

    # Mappa subset → directory HDFS
    case "$S" in
      pct_025) DIR="/data/subsets/pct_025" ;;
      pct_050) DIR="/data/subsets/pct_050" ;;
      pct_100) DIR="/data/subsets/pct_100" ;;
      pct_200) DIR="/data/subsets/pct_200" ;;
      full)    DIR="/data/cleaned" ;;
    esac

    echo ""
    echo "▶▶▶ Avvio esecuzione completa per subset: ${S}"

    # ── JOB 3.1 — Statistiche Compagnie Aeree ────────────────────────────
    echo ""
    echo "──────────────────────────────────────────────"
    echo "  JOB 3.1 — Statistiche Compagnie Aeree"
    echo "──────────────────────────────────────────────"
    run_spark_job "Job31_SparkSQL_${S}"  "SparkSQL"  "01_job31_spark_sql.py"  "job31" "${S}"
    run_spark_job "Job31_SparkCore_${S}" "SparkCore" "01_job31_spark_core.py" "job31" "${S}"
    run_hive_job  "Job31" "job31" "jobs/01_job31_hive.hql" "${S}" "${DIR}"

    # ── JOB 3.2 — Report Ritardi e Cause ────────────────────────────────
    echo ""
    echo "──────────────────────────────────────────────"
    echo "  JOB 3.2 — Report Ritardi e Cause"
    echo "──────────────────────────────────────────────"
    run_spark_job "Job32_SparkCore_${S}" "SparkCore" "02_job32_spark_core.py" "job32" "${S}"
    run_spark_job "Job32_SparkSQL_${S}"  "SparkSQL"  "02_job32_spark_sql.py"  "job32" "${S}"
    run_hive_job  "Job32" "job32" "jobs/02_job32_hive.hql" "${S}" "${DIR}"

    # ── JOB 3.3 — Ranking Anomalie ──────────────────────────────────────
    echo ""
    echo "──────────────────────────────────────────────"
    echo "  JOB 3.3 — Ranking Anomalie"
    echo "──────────────────────────────────────────────"
    run_spark_job "Job33_SparkCore_${S}" "SparkCore" "03_job33_spark_core.py" "job33" "${S}"
    run_spark_job "Job33_SparkSQL_${S}"  "SparkSQL"  "03_job33_spark_sql.py"  "job33" "${S}"
    run_hive_job  "Job33" "job33" "jobs/03_job33_hive.hql" "${S}" "${DIR}"
}

# ─── Header ──────────────────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════╗"
echo "║  BIG DATA PROJECT — Esecuzione locale        ║"
echo "║  Subset: ${SUBSET} | Log: ${LOGDIR}          ║"
echo "║  Tecnologie: SparkSQL, SparkCore, Hive       ║"
echo "╚══════════════════════════════════════════════╝"

# ─── Step 0: Genera i subset (solo se non esistono già) ──────────────────────
echo ""
echo "═══════════════════════════════════════════════"
echo "  Generazione subset (pct_025/050/100/200)"
echo "═══════════════════════════════════════════════"
docker exec spark-master ${SPARK_SUBMIT} \
    --master spark://spark-master:7077 \
    /jobs/generate_subsets.py 2>&1 | tee ${LOGDIR}/GenerateSubsets.log

# ─── Step 1: Data Cleaning (opera sempre sul dataset completo) ────────────────
run_spark_job "DataCleaning" "PySpark" "00_data_cleaning.py" "cleaning"

# ─── Step 2–4: Esecuzione Job per ogni subset ─────────────────────────────────
if [ "${SUBSET}" = "all" ]; then
    echo ""
    echo "╔══════════════════════════════════════════════╗"
    echo "║  Modalità BENCHMARKING — tutti i subset      ║"
    echo "╚══════════════════════════════════════════════╝"

    # Crea/aggiorna il file consolidato all
    TIMING_ALL="./results/timings_local_all.txt"
    echo "job,technology,subset,environment,elapsed_seconds" > "${TIMING_ALL}"

    for S in pct_025 pct_050 pct_100 pct_200; do
        echo ""
        echo "╔══════════════════════════════════════════════╗"
        echo "║  Subset: ${S}                                ║"
        echo "╚══════════════════════════════════════════════╝"
        run_all_for_subset "${S}"

        # Accoda i timing del subset nel file consolidato all
        tail -n +2 "${TIMING_FILE}" >> "${TIMING_ALL}"
    done

else
    run_all_for_subset "${SUBSET}"
fi

# ─── Riepilogo finale ────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  ✅ TUTTI I JOB COMPLETATI                   ║"
echo "║  Tempi: ${TIMING_FILE}                       ║"
echo "╚══════════════════════════════════════════════╝"
cat ${TIMING_FILE}
