#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

"""
convert_hive_parquet.py
Converte tutti i file Parquet di output Hive in file CSV leggibili.
I CSV vengono salvati nella stessa cartella con il nome results.csv.

Uso:
    python analysis/convert_hive_parquet.py
"""

import os
import sys
import glob
import pandas as pd

BASE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "outputs", "aws_outputs", "risultati_hive"
)

def is_parquet(filepath):
    """Controlla il magic number PAR1 dei file Parquet."""
    try:
        with open(filepath, "rb") as f:
            header = f.read(4)
        return header == b"PAR1"
    except Exception:
        return False

def convert_folder(folder_path):
    """
    Trova tutti i file Parquet in una cartella,
    li legge e li combina in un unico results.csv.
    """
    parquet_files = []
    for entry in os.listdir(folder_path):
        full_path = os.path.join(folder_path, entry)
        if os.path.isfile(full_path) and not entry.startswith("_") and is_parquet(full_path):
            parquet_files.append(full_path)

    if not parquet_files:
        return False

    dfs = []
    for pf in sorted(parquet_files):
        try:
            df = pd.read_parquet(pf)
            dfs.append(df)
        except Exception as e:
            print(f"    [WARN] Impossibile leggere {os.path.basename(pf)}: {e}")

    if not dfs:
        return False

    combined = pd.concat(dfs, ignore_index=True)
    out_csv = os.path.join(folder_path, "results.csv")
    combined.to_csv(out_csv, index=False)
    return out_csv, combined

def main():
    if not os.path.isdir(BASE_DIR):
        print(f"[ERROR] Cartella non trovata: {BASE_DIR}")
        sys.exit(1)

    print(f"Scansione: {BASE_DIR}\n")
    converted = 0
    failed = 0

    # Cammina ricorsivamente
    for root, dirs, files in os.walk(BASE_DIR):
        # Salta cartelle che non contengono file (solo sottocartelle)
        actual_files = [f for f in files if not f.startswith("_")]
        if not actual_files:
            continue

        result = convert_folder(root)
        if result and result is not False:
            out_csv, df = result
            rel = os.path.relpath(out_csv, BASE_DIR)
            print(f"  [OK] {rel}")
            print(f"       Righe: {len(df)}  |  Colonne: {list(df.columns)}")
            print()
            converted += 1
        else:
            failed += 1

    print("-" * 60)
    print(f"Conversioni completate: {converted}")
    if failed:
        print(f"Cartelle senza Parquet validi: {failed}")

if __name__ == "__main__":
    main()
