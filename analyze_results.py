"""
Statistical analysis of drift detection evaluation results.
Computes mean +/- std per configuration.

Adds:
  --by-dataset       group by Dataset as well (needed to verify Table 5.1)
  --table51          print the exact cells that populate the cross-dataset table
  D1/D2/R are per-run values in the CSV (same value repeated across a run's
  drift-type rows), so per-type means of them are informational only; F1,
  precision, recall are the genuinely per-drift-type quantities.
"""

import argparse
import csv
import math
from collections import defaultdict


def parse_float(val):
    if val is None:
        return None
    val = val.strip()
    if val.lower() == "nan" or val == "":
        return None
    try:
        return float(val)
    except ValueError:
        return None


def mean(values):
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def std(values):
    # population-consistent sample std (ddof=1), matching the original script
    vals = [v for v in values if v is not None]
    n = len(vals)
    if n < 2:
        return None
    m = sum(vals) / n
    variance = sum((v - m) ** 2 for v in vals) / (n - 1)
    return math.sqrt(variance)


def fmt(v, decimals=4):
    return "nan" if v is None else f"{v:.{decimals}f}"


def load_records(path):
    records = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames or []
        has_dataset = "Dataset" in fields
        has_d1 = "D1" in fields
        for row in reader:
            det = (row.get("Detector") or "").strip()
            if det not in ("mmd", "padd"):
                continue
            drift_type = (row.get("DriftType") or "").strip()
            records.append({
                "dataset":    (row.get("Dataset", "unknown") or "unknown").strip() if has_dataset else "unknown",
                "detector":   det,
                "trigger_n":  (row.get("TriggerN", "100") or "100").strip(),
                "drift_type": drift_type,
                "precision":  parse_float(row.get("Precision")),
                "recall":     parse_float(row.get("Recall")),
                "f1":         parse_float(row.get("F1")),
                "lat_docs":   parse_float(row.get("LatencyDocs")),
                "far":        parse_float(row.get("FAR")),
                "d1":         parse_float(row.get("D1")) if has_d1 else None,
                "d2":         parse_float(row.get("D2")) if has_d1 else None,
                "r":          parse_float(row.get("R"))  if has_d1 else None,
            })
    return records


def table51(records):
    """Print the exact per-dataset F1 and FAR cells for the cross-dataset table."""
    # per-drift-type F1: exclude the __GLOBAL__ summary rows
    per_type = defaultdict(list)   # (dataset, detector, drift_type) -> [f1...]
    far_run  = defaultdict(list)   # (dataset, detector) -> [far per run]  (far is per-run)

    for r in records:
        if r["trigger_n"] != "100":
            continue
        if r["drift_type"] == "__GLOBAL__":
            # use the global row once per run for the run-level FAR
            far_run[(r["dataset"], r["detector"])].append(r["far"])
            continue
        per_type[(r["dataset"], r["detector"], r["drift_type"])].append(r["f1"])

    datasets  = ["newsgroups", "yahoo", "agnews"]
    detectors = ["mmd", "padd"]
    types     = ["sudden", "gradual", "recurring"]

    print(f"{'Dataset':<12} {'Det':<5} "
          f"{'Sudden F1':>18} {'Gradual F1':>18} {'Recurring F1':>18} "
          f"{'FAR':>10} {'n':>4}")
    print("-" * 90)
    for ds in datasets:
        for det in detectors:
            cells = []
            n_seen = None
            for t in types:
                vals = per_type.get((ds, det, t), [])
                n_seen = len(vals) if vals else n_seen
                cells.append(f"{fmt(mean(vals),3)} +/- {fmt(std(vals),3)}")
            far_vals = far_run.get((ds, det), [])
            far_mean = mean(far_vals)
            print(f"{ds:<12} {det:<5} "
                  f"{cells[0]:>18} {cells[1]:>18} {cells[2]:>18} "
                  f"{fmt(far_mean,5):>10} {str(n_seen or 0):>4}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="evaluation_results.csv")
    ap.add_argument("--table51", action="store_true",
                    help="Print the per-dataset cells that populate Table 5.1")
    args = ap.parse_args()

    records = load_records(args.input)
    print(f"Loaded {len(records)} detector rows from {args.input}\n")
    if args.table51:
        table51(records)
    else:
        print("Run with --table51 to print the cross-dataset table cells.")


if __name__ == "__main__":
    main()