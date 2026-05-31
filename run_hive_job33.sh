#!/bin/bash
# run_hive_job33.sh — Esegue il Job 3.3 in Hive per un subset specifico o per tutti
#
# Uso:
#   bash run_hive_job33.sh [subset] [base_path]
#
# [subset] può essere:
#   pct_025 (o 25)  → 25% del dataset
#   pct_050 (o 50)  → 50% del dataset
#   pct_100 (o 100) → dataset completo
#   pct_200 (o 200) → dataset 2x replicato
#   full (o cleaned)→ dataset completo da cartella cleaned (default)
#   all             → esecuzione benchmark di tutti i subset

set -e
set -o pipefail

# ─── Normalizzazione subset: accetta sia "pct_025" che "25" ──────────────────
raw_subset=${1:-full}
case "$raw_subset" in
  25|pct_25|pct_025)   SUBSET="pct_025"; INPUT_DIR="/data/subsets/pct_025" ;;
  50|pct_50|pct_050)   SUBSET="pct_050"; INPUT_DIR="/data/subsets/pct_050" ;;
  100|pct_100)         SUBSET="pct_100"; INPUT_DIR="/data/subsets/pct_100" ;;
  200|pct_200)         SUBSET="pct_200"; INPUT_DIR="/data/subsets/pct_200" ;;
  full|cleaned)        SUBSET="full";    INPUT_DIR="/data/cleaned" ;;
  all)                 SUBSET="all" ;;
  *) echo "[WARN] Subset '${raw_subset}' non riconosciuto. Uso full."; SUBSET="full"; INPUT_DIR="/data/cleaned" ;;
esac

# ─── Configurazione BASE_PATH (HDFS o S3) ───────────────────────────────────
BASE_PATH="hdfs://namenode:9000"
if [ -n "$2" ]; then
  BASE_PATH="${2%/}"
fi

LOGDIR="./logs/hive_job33_$(date +%Y%m%d_%H%M%S)"
mkdir -p ${LOGDIR}

TIMING_FILE="./results/timings_local_${SUBSET}.txt"
mkdir -p ./results

# Crea il file di timing se non esiste con gli header corretti
if [ ! -f "${TIMING_FILE}" ]; then
  echo "job,technology,subset,environment,elapsed_seconds" > "${TIMING_FILE}"
fi

# ─── Funzione per eseguire Hive per un singolo subset ────────────────────────
run_hive_subset() {
    local SUB=$1
    local DIR=$2

    echo ""
    echo "═══════════════════════════════════════════════"
    echo "  Eseguendo: Job 3.3 (Hive HQL)  [subset=${SUB}]"
    echo "  Directory sorgente HDFS: ${DIR}"
    echo "═══════════════════════════════════════════════"

    T_START=$(python3 -c 'import time; print(int(time.time() * 1000))')

    # Invia il file HQL tramite standard input a Beeline in hiveserver2
    docker exec -i hiveserver2 beeline \
        -u jdbc:hive2://localhost:10000 \
        -n hive \
        --hivevar BASE_PATH="${BASE_PATH}" \
        --hivevar SUBSET="${SUB}" \
        --hivevar INPUT_DIR="${DIR}" \
        < jobs/03_job33_hive.hql 2>&1 | tee "${LOGDIR}/Job33_hive_${SUB}.log"

    T_END=$(python3 -c 'import time; print(int(time.time() * 1000))')
    ELAPSED=$(awk "BEGIN {printf \"%.1f\", ($T_END - $T_START) / 1000}")

    # Scrive i timing nel file del subset specifico
    echo "job33,hive,${SUB},local,${ELAPSED}" >> "${TIMING_FILE}"

    # Se il timing generale timings_local_all.txt esiste e non siamo già in "all", scrive anche lì
    if [ "${SUBSET}" != "all" ] && [ -f "./results/timings_local_all.txt" ]; then
      echo "job33,hive,${SUB},local,${ELAPSED}" >> "./results/timings_local_all.txt"
    fi

    echo "  ✅ Job 3.3 Hive (${SUB}) completato in: ${ELAPSED}s"
}

# ─── Esecuzione ─────────────────────────────────────────────────────────────
if [ "${SUBSET}" = "all" ]; then
    echo "╔══════════════════════════════════════════════╗"
    echo "║  Modalità BENCHMARKING Hive — tutti i subset ║"
    echo "╚══════════════════════════════════════════════╝"
    for S in pct_025 pct_050 pct_100 pct_200; do
        case "$S" in
          pct_025) DIR="/data/subsets/pct_025" ;;
          pct_050) DIR="/data/subsets/pct_050" ;;
          pct_100) DIR="/data/subsets/pct_100" ;;
          pct_200) DIR="/data/subsets/pct_200" ;;
        esac
        run_hive_subset "$S" "$DIR"
    done
else
    run_hive_subset "${SUBSET}" "${INPUT_DIR}"
fi

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  ✅ PROCESSO COMPLETATO                     ║"
echo "║  Tempi salvati in: ${TIMING_FILE}            ║"
echo "╚══════════════════════════════════════════════╝"
cat "${TIMING_FILE}"
