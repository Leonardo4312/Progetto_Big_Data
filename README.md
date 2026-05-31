# Flight Delay 2024 — Big Data Analysis
## Università Roma Tre · Corso di Big Data · Prof. Torlone

> Analisi comparativa di tecnologie Big Data (Spark SQL, Spark Core, Hive)
> sul dataset Flight Delay 2024 (7M+ record).

---

## 📁 Struttura del Repository

```
.
├── docker-compose.yml          # Ecosistema Hadoop + Hive + Spark
├── hadoop.env                  # Variabili ambiente per i container
├── setup_hdfs.sh               # Carica dataset su HDFS
├── run_hive_job31.sh           # Lancia Job 3.1 via beeline (Hive)
├── run_hive_job33.sh           # Lancia Job 3.3 via beeline (Hive)
├── aws_guide.sh                # Workflow completo AWS EMR
│
├── jobs/
│   ├── 00_data_cleaning.py     # Preparazione dati (PySpark DataFrame)
│   ├── 01_job31_*              # Job 3.1 — Script per Spark SQL, Spark Core e Hive
│   ├── 02_job32_*              # Job 3.2 — Script per Spark SQL, Spark Core e Hive
│   ├── 03_job33_*              # Job 3.3 — Script per Spark SQL, Spark Core e Hive
│   └── generate_subsets.py     # Script per generare i sotto-campioni (25%, 50%, 100%, 200%)
│
├── analysis/
│   ├── convert_hive_parquet.py # Utilità di conversione per i formati
│   └── report_benchmark.md     # Documento di analisi delle prestazioni
│
├── logs/                       # Log di esecuzione locale (contenenti le prime 10 righe di output)
│   ├── local_pct_025/          # Log esecuzioni sul 25% del dataset
│   └── local_pct_.../          # (cartelle analoghe per 50%, 100%, 200%)
│
├── outputs/
│   └── aws_outputs/            # Output e log completi salvati da AWS EMR
│
├── results/
│   ├── charts/                 # Grafici PNG generati per il rapporto
│   └── timings_local_*.txt     # Tempi di esecuzione aggregati per subset
│
├── data/
│   ├── flights_2024.csv        # Dataset principale scaricato (~1.3 GB, non tracciato)
│   └── .gitignore              # Regole di esclusione per file pesanti
│
└── README.md
```

---

## 🔬 Riproducibilità dell'Esperimento

Per garantire la **totale riproducibilità** dell'esperimento, l'ambiente di esecuzione locale è stato interamente dockerizzato. Tutte le dipendenze software, le versioni di Hadoop/Spark/Hive e le configurazioni di memoria sono fissate all'interno di `docker-compose.yml` e dei file shell di orchestrazione. Questo garantisce che i risultati presentati nella relazione (eseguiti sui dataset al 25%, 50%, 100% e 200%) possano essere replicati fedelmente su qualsiasi altra macchina host annullando eventuali problematiche legate a dipendenze locali.

### Specifiche e Prerequisiti di Sistema
Per un'esecuzione stabile e per evitare errori di *Out Of Memory*, si raccomanda:
- **Docker Engine** ≥ 20.10 e **Docker Compose** ≥ 2.0.
- Almeno **12 GB di RAM** liberi allocabili per Docker (i container Spark e Hive sono stati tarati per richiedere un massimo di ~8GB complessivi durante le esecuzioni più onerose).
- **Dataset Originale**: Il file originario pesa circa 700MB. Per la riproduzione, scaricare il [Flight Delay Dataset 2024](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024) da Kaggle e posizionarlo in `./data/flights_2024.csv`.

---

## 🚀 Quick Start — Esecuzione e Replicazione Locale

### 1. Download del Dataset
Come prima operazione, scarica il dataset da Kaggle. Assicurati di rinominare il file CSV (se necessario) e posizionarlo esattamente in questo path all'interno del progetto:
```bash
# Il path finale del CSV DEVE essere esattamente questo:
./data/flights_2024.csv
```

### 2. Avvio dell'Infrastruttura (Docker)
L'intera infrastruttura big data (HDFS, Spark Master/Worker, HiveServer2) viene istanziata in automatico.

```bash
# Clona il repository e posizionati nella cartella root
git clone https://github.com/<tuo-username>/bigdata-flight2024.git
cd bigdata-flight2024

# Costruisci e avvia l'intero ecosistema in background
docker-compose up -d --build

# È raccomandato attendere circa 60 secondi per l'inizializzazione completa dei servizi.
# Puoi verificare lo stato dei container con:
docker-compose ps
```

**Interfacce di monitoraggio attive:**
- **HDFS NameNode**: [http://localhost:9870](http://localhost:9870)
- **Spark Master**: [http://localhost:8080](http://localhost:8080)
- **HiveServer2**: [http://localhost:10002](http://localhost:10002)

### 3. Inizializzazione HDFS
Lo script `setup_hdfs.sh` crea le folder necessarie nel file system distribuito e carica il dataset grezzo caricato precedentemente.

```bash
bash setup_hdfs.sh
# Output atteso alla fine: "✅ Setup completato. Dataset disponibile in HDFS."
```

### 4. Riproduzione del Benchmarking Completo
Per replicare **esattamente** i risultati documentati nella relazione, è stato predisposto un robusto script orchestratore (`run_all_local.sh`). Questo script in totale autonomia:
1. Pulisce e prepara i dati attivando il file `00_data_cleaning.py`.
2. Genera i sotto-campioni richiesti al 25%, 50%, 100% e 200%.
3. Esegue tutti i Job (3.1, 3.2, 3.3) incrociandoli con tutte le tecnologie (Spark SQL, Spark Core RDD, Hive).
4. Misura isolatamente i tempi di esecuzione riversandoli nei file di recap temporale.

Per riprodurre lo scenario completo della relazione (consigliato, ma richiede tempo poiché elabora un totale di 36 combinazioni):
```bash
# Esegue l'intera suite di benchmarking su tutti i subset
bash run_all_local.sh all
```

*(Opzionale)* Per riprodurre, ad esempio, solo il subset al 25% (indicato come riferimento riassuntivo nella relazione per motivi di spazio ed eleganza espositiva):
```bash
bash run_all_local.sh 25
```

Tutti i log puntuali verranno posizionati nella cartella `./logs/` appena creata, mentre i tempi di esecuzione finali saranno in `results/timings_local_*.txt`.

### 5. Generazione dei Grafici di Confronto
Una volta terminate le elaborazioni descritte al punto 4, puoi autogenerare i grafici a barre dei tempi di risposta utilizzati nel documento PDF:

```bash
# Assicurati di avere le dipendenze Python installate localmente sulla tua macchina
pip install pandas matplotlib seaborn

# Esegui lo script generatore di grafici
python analysis/plot_timings.py

# I nuovi grafici aggiornati verranno depositati nella directory:
ls -la results/charts/
```

---

## ☁️ Esecuzione su AWS EMR

Vedi [`aws_guide.sh`](./aws_guide.sh) per la guida completa step-by-step. Di seguito i passaggi principali per il caricamento dei dati e l'esecuzione.

### Prerequisiti
- AWS CLI configurato (`aws configure`)
- Chiave EC2 disponibile nella regione target
- Credenziali con permessi EMR + S3

### 1. Caricamento dati e script su S3
Crea un bucket S3 e carica il dataset, gli script Python e i file necessari per l'esecuzione:

```bash
# Sostituisci <tuo-bucket> con il nome del tuo bucket
aws s3 mb s3://<tuo-bucket>

# Caricamento del dataset
aws s3 cp data/flights_2024.csv s3://<tuo-bucket>/data/

# Caricamento degli script di lavoro
aws s3 cp jobs/ s3://<tuo-bucket>/jobs/ --recursive
```

### 2. Creazione del Cluster EMR
Avvia un cluster EMR tramite AWS Management Console o CLI selezionando:
- Applicazioni: Spark, Hadoop e Hive
- Tipo istanze: `m5.xlarge` (1 Master, 2 Core)

### 3. Sottomissione dei Job su EMR
Puoi collegarti al nodo Master via SSH (utilizzando la tua chiave EC2) per eseguire i job, oppure aggiungerli come "Step" dalla console AWS.

Esempio di esecuzione manuale via SSH dal nodo Master:
```bash
spark-submit \
  --deploy-mode cluster \
  --executor-memory 4G \
  --executor-cores 2 \
  s3://<tuo-bucket>/jobs/01_job31_spark_sql.py \
  s3://<tuo-bucket>/data/flights_2024.csv \
  s3://<tuo-bucket>/output/job31/
```

---

## 📊 Dataset

- **Fonte**: [Kaggle — Flight Delay Dataset 2024](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024)
- **Dimensione**: ~7 milioni di record, 35 colonne, formato CSV
- **Colonne utilizzate**: 14 (vedi `jobs/00_data_cleaning.py`)

> ⚠️ Il file CSV non è incluso nel repository per dimensioni.
> Scaricarlo da Kaggle e posizionarlo in `./data/flights_2024.csv`.

---



## 📈 Risultati (Prime 10 righe)

Le prime 10 righe di output prodotte da ogni job (a dimostrazione della corretta elaborazione) sono stampate all'interno dei file di log testuali posizionati nella cartella `logs/local_pct_XXX/` (ad esempio nel file `logs/local_pct_025/Job31_SparkSQL_pct_025.log`).

Gli output completi relativi alle esecuzioni sul cluster cloud, invece, sono archiviati all'interno della directory `outputs/aws_outputs/`.

---

## 👥 Autori

- Leonardo Anatra - Matricola 552932
- Edoardo Piazzolla - Matricola 577068
- Corso: Big Data, A.A. 2025/2026
- Docente: Prof. Riccardo Torlone
