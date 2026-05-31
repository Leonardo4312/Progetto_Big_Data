# Benchmark dei Job Big Data — Confronto Tecnologie e Ambienti

## Indice
1. [Introduzione](#introduzione)
2. [Tabelle Tempi — Ambiente Locale](#tabelle-tempi--ambiente-locale)
3. [Tabelle Tempi — Ambiente AWS](#tabelle-tempi--ambiente-aws)
4. [Grafici di Confronto](#grafici-di-confronto)
5. [Osservazioni e Conclusioni](#osservazioni-e-conclusioni)

---

## Introduzione

Il benchmark confronta tre tecnologie (**Spark SQL**, **Spark Core RDD**, **Hive**) su tre job distinti, eseguiti su quattro subset del dataset di voli 2024:

| Subset | Record approssimativi |
|--------|----------------------|
| 25%    | ~1.77 M              |
| 50%    | ~3.54 M              |
| 100%   | ~7.08 M              |
| 200%   | ~14.16 M             |

Ambienti testati: **Docker locale** (single-node) e **AWS EMR** (cluster distribuito).

---

## Tabelle Tempi — Ambiente Locale

### Job 3.1 — Statistiche Compagnie Aeree

| Tecnologia  | 25%   | 50%   | 100%  | 200%  |
|-------------|------:|------:|------:|------:|
| Spark SQL   | 45.1s | 48.2s | 48.0s | 65.7s |
| Spark Core  | 35.2s | 45.2s | 48.8s | 87.4s |
| Hive        | 10.9s | 11.4s | 15.4s | 28.9s |

### Job 3.2 — Report Ritardi per Aeroporto

| Tecnologia  | 25%   | 50%   | 100%  | 200%   |
|-------------|------:|------:|------:|-------:|
| Spark SQL   | 63.7s | 70.3s | 68.0s |  71.1s |
| Spark Core  | 55.8s | 79.5s | 89.3s | 146.8s |
| Hive        | 24.8s | 27.7s | 36.1s |  52.7s |

### Job 3.3

| Tecnologia  | 25%   | 50%   | 100%  | 200%   |
|-------------|------:|------:|------:|-------:|
| Spark SQL   | 52.0s | 56.1s | 56.9s |  49.1s |
| Spark Core  | 43.9s | 58.3s | 73.8s | 119.4s |
| Hive        | 19.1s | 22.8s | 30.1s |  45.1s |

### Totale (somma tutti i job) — Locale

| Tecnologia  | 25%    | 50%    | 100%   | 200%   |
|-------------|-------:|-------:|-------:|-------:|
| Spark SQL   | 160.8s | 174.6s | 172.9s | 185.9s |
| Spark Core  | 134.9s | 183.0s | 211.9s | 353.6s |
| Hive        |  54.8s |  61.9s |  81.6s | 126.7s |

---

## Tabelle Tempi — Ambiente AWS

### Job 3.1 — Statistiche Compagnie Aeree

| Tecnologia  | 25%   | 50%   | 100%  | 200%  |
|-------------|------:|------:|------:|------:|
| Spark SQL   | 29.0s | 30.4s | 34.6s | 41.3s |
| Spark Core  | 33.5s | 41.9s | 58.3s | 81.6s |
| Hive (EMR)  | 58.0s | 58.0s | 56.0s | 78.0s |

### Job 3.2 — Report Ritardi per Aeroporto

| Tecnologia  | 25%   | 50%   | 100%  | 200%   |
|-------------|------:|------:|------:|-------:|
| Spark SQL   | 32.2s | 32.6s | 34.0s |  37.4s |
| Spark Core  | 41.9s | 55.9s | 83.7s | 141.1s |
| Hive (EMR)  | 62.0s | 60.0s | 54.0s |  76.0s |

### Job 3.3

| Tecnologia  | 25%   | 50%   | 100%  | 200%   |
|-------------|------:|------:|------:|-------:|
| Spark SQL   | 31.4s | 31.7s | 33.2s |  38.3s |
| Spark Core  | 42.3s | 55.4s | 79.7s | 110.5s |
| Hive (EMR)  | 51.0s | 56.0s | 64.0s |  90.0s |

### Totale (somma tutti i job) — AWS

| Tecnologia  | 25%    | 50%    | 100%   | 200%   |
|-------------|-------:|-------:|-------:|-------:|
| Spark SQL   |  92.6s |  94.7s | 101.8s | 117.0s |
| Spark Core  | 117.7s | 153.2s | 221.7s | 333.2s |
| Hive (EMR)  | 171.0s | 174.0s | 174.0s | 244.0s |

---

## Confronto Locale vs AWS — Subset 100%

| Job     | Tecnologia  | Locale | AWS   | Speedup AWS |
|---------|-------------|-------:|------:|------------:|
| Job 3.1 | Spark SQL   | 48.0s  | 34.6s | **1.39x**   |
| Job 3.1 | Spark Core  | 48.8s  | 58.3s | 0.84x       |
| Job 3.1 | Hive        | 15.4s  | 56.0s | 0.28x       |
| Job 3.2 | Spark SQL   | 68.0s  | 34.0s | **2.00x**   |
| Job 3.2 | Spark Core  | 89.3s  | 83.7s | 1.07x       |
| Job 3.2 | Hive        | 36.1s  | 54.0s | 0.67x       |
| Job 3.3 | Spark SQL   | 56.9s  | 33.2s | **1.71x**   |
| Job 3.3 | Spark Core  | 73.8s  | 79.7s | 0.93x       |
| Job 3.3 | Hive        | 30.1s  | 64.0s | 0.47x       |

> Speedup > 1 = AWS più veloce. Speedup < 1 = Locale più veloce.

---

## Grafici di Confronto

### G1 — Confronto tecnologie per job (Locale)
![G1](g1_local_tech_comparison.png)

### G2 — Confronto tecnologie per job (AWS)
![G2](g2_aws_tech_comparison.png)

### G3 — Scalabilità al crescere del dataset (Locale)
![G3](g3_local_scalability.png)

### G4 — Scalabilità al crescere del dataset (AWS)
![G4](g4_aws_scalability.png)

### G5 — Locale vs AWS per tecnologia (Subset 100%)
![G5](g5_local_vs_aws_pct100.png)

### G6 — Heatmap tempi AWS
![G6](g6_heatmap_aws.png)

### G7 — Heatmap tempi Locale
![G7](g7_heatmap_local.png)

### G8 — Speedup AWS vs Locale (Subset 100%)
![G8](g8_speedup_aws_vs_local.png)

### G9 — Tempo totale per tecnologia (Subset 100%)
![G9](g9_total_by_tech.png)

---

## Osservazioni e Conclusioni

### Hive in locale è sorprendentemente veloce
In ambiente locale Hive risulta costantemente il più rapido (es. Job 3.1: 15.4s vs 48s di Spark), grazie al fatto che su dataset di medie dimensioni il motore MapReduce su HDFS locale ha overhead minimo. Su AWS EMR invece il cluster introduce latenza di coordinamento che penalizza Hive rispetto a Spark.

### Spark SQL domina su AWS
Su AWS Spark SQL è la tecnologia più efficiente in assoluto, con tempi quasi costanti al crescere del dataset (34–41s per Job 3.1 anche a 200%) grazie all'ottimizzazione del Catalyst optimizer e alla parallelizzazione distribuita.

### Spark Core non scala bene
Spark Core RDD mostra la peggiore scalabilità: a 200% supera i 140s su Job 3.2 sia in locale che su AWS. L'assenza dell'ottimizzatore automatico e la gestione manuale delle aggregazioni ne limitano le performance su dataset grandi.

### AWS non conviene sempre
Per dataset piccoli (25%–50%) e job semplici (Job 3.1), il costo di avvio del cluster EMR e la latenza di rete rendono il locale competitivo o superiore. AWS mostra il suo vantaggio reale su dataset grandi (200%) con Spark SQL.

### Hive su AWS: overhead di cluster
I tempi Hive su AWS EMR (58–90s) sono superiori al locale (10–45s) perché il motore Hive-on-MR su EMR include il costo di avvio dei container YARN e la comunicazione tra nodi, assente in locale.
