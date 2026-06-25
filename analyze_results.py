"""
Statistical analysis of drift detection evaluation results.
Computes mean +/- std per configuration (Detector, TriggerN, DriftType).
"""

import argparse
import csv
import math
import sys
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
    if not vals:
        return None
    return sum(vals) / len(vals)


def std(values):
    vals = [v for v in values if v is not None]
    n = len(vals)
    if n < 2:
        return None
    m = sum(vals) / n
    variance = sum((v - m) ** 2 for v in vals) / (n - 1)
    return math.sqrt(variance)


def fmt(v, decimals=4):
    if v is None:
        return "nan"
    return f"{v:.{decimals}f}"


def load_records(path):
    records = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        has_dataset = "Dataset" in fieldnames
        has_d1 = "D1" in fieldnames

        for row in reader:
            # Skip malformed rows
            if not row.get("Detector") or row["Detector"] not in ("mmd", "padd"):
                continue

            r = {
                "dataset":    row.get("Dataset", "unknown").strip() if has_dataset else "unknown",
                "detector":   row["Detector"].strip(),
                "trigger_n":  row.get("TriggerN", "100").strip(),
                "drift_type": row.get("DriftType", "").strip(),
                "precision":  parse_float(row.get("Precision")),
                "recall":     parse_float(row.get("Recall")),
                "f1":         parse_float(row.get("F1")),
                "lat_docs":   parse_float(row.get("LatencyDocs")),
                "lat_ms":     parse_float(row.get("LatencyMs")),
                "far":        parse_float(row.get("FAR")),
                "d1":         parse_float(row.get("D1")) if has_d1 else None,
                "d2":         parse_float(row.get("D2")) if has_d1 else None,
                "r":          parse_float(row.get("R")) if has_d1 else None,
            }
            records.append(r)
    return records


def group_records(records, group_by_dataset):
    groups = defaultdict(list)
    for r in records:
        if group_by_dataset:
            key = (r["dataset"], r["detector"], r["trigger_n"], r["drift_type"])
        else:
            key = (r["detector"], r["trigger_n"], r["drift_type"])
        groups[key].append(r)
    return groups


def compute_stats(group):
    metrics = ["precision", "recall", "f1", "lat_docs", "lat_ms", "far", "d1", "d2", "r"]
    stats = {}
    for m in metrics:
        vals = [r[m] for r in group]
        stats[f"{m}_mean"] = mean(vals)
        stats[f"{m}_std"]  = std(vals)
        stats[f"{m}_n"]    = len([v for v in vals if v is not None])
    return stats


def print_table(sorted_keys, groups, group_by_dataset):
    if group_by_dataset:
        hdr = f"{'Dataset':<12} {'Det':<6} {'TrigN':<6} {'DriftType':<10} {'N':>3}"
    else:
        hdr = f"{'Det':<6} {'TrigN':<6} {'DriftType':<10} {'N':>3}"

    hdr += (
        f"  {'F1 mean':>8} {'+-':>4} {'std':>6}"
        f"  {'Prec mean':>9} {'+-':>4} {'std':>6}"
        f"  {'FAR mean':>10} {'+-':>4} {'std':>8}"
        f"  {'LatDocs mean':>12} {'+-':>4} {'std':>7}"
        f"  {'D1 mean':>8} {'+-':>4} {'std':>7}"
        f"  {'D2 mean':>8} {'+-':>4} {'std':>7}"
        f"  {'R mean':>7} {'+-':>4} {'std':>6}"
    )
    print(hdr)
    print("-" * len(hdr))

    output_rows = []
    for key in sorted_keys:
        group = groups[key]
        s = compute_stats(group)
        n = s["f1_n"]

        if group_by_dataset:
            dataset, det, tn, dt = key
            prefix = f"{dataset:<12} {det:<6} {tn:<6} {dt:<10} {n:>3}"
        else:
            det, tn, dt = key
            prefix = f"{det:<6} {tn:<6} {dt:<10} {n:>3}"

        line = (
            prefix +
            f"  {fmt(s['f1_mean']):>8}  +- {fmt(s['f1_std']):>6}"
            f"  {fmt(s['precision_mean']):>9}  +- {fmt(s['precision_std']):>6}"
            f"  {fmt(s['far_mean'], 6):>10}  +- {fmt(s['far_std'], 6):>8}"
            f"  {fmt(s['lat_docs_mean'], 1):>12}  +- {fmt(s['lat_docs_std'], 1):>7}"
            f"  {fmt(s['d1_mean'], 2):>8}  +- {fmt(s['d1_std'], 2):>7}"
            f"  {fmt(s['d2_mean'], 2):>8}  +- {fmt(s['d2_std'], 2):>7}"
            f"  {fmt(s['r_mean'], 4):>7}  +- {fmt(s['r_std'], 4):>6}"
        )
        print(line)

        row = {
            "Detector":       det,
            "TriggerN":       tn,
            "DriftType":      dt,
            "N_runs":         n,
            "F1_mean":        fmt(s["f1_mean"]),
            "F1_std":         fmt(s["f1_std"]),
            "Precision_mean": fmt(s["precision_mean"]),
            "Precision_std":  fmt(s["precision_std"]),
            "Recall_mean":    fmt(s["recall_mean"]),
            "Recall_std":     fmt(s["recall_std"]),
            "FAR_mean":       fmt(s["far_mean"], 6),
            "FAR_std":        fmt(s["far_std"], 6),
            "LatDocs_mean":   fmt(s["lat_docs_mean"], 1),
            "LatDocs_std":    fmt(s["lat_docs_std"], 1),
            "LatMs_mean":     fmt(s["lat_ms_mean"], 1),
            "LatMs_std":      fmt(s["lat_ms_std"], 1),
            "D1_mean":        fmt(s["d1_mean"], 2),
            "D1_std":         fmt(s["d1_std"], 2),
            "D2_mean":        fmt(s["d2_mean"], 2),
            "D2_std":         fmt(s["d2_std"], 2),
            "R_mean":         fmt(s["r_mean"], 4),
            "R_std":          fmt(s["r_std"], 4),
        }
        if group_by_dataset:
            row = {"Dataset": key[0], **row}
        output_rows.append(row)

    return output_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",   default="evaluation_results.csv")
    parser.add_argument("--output",  default="stats_summary.csv")
    parser.add_argument("--by-dataset", action="store_true",
                        help="Group by Dataset column (requires Dataset column in CSV)")
    args = parser.parse_args()

    print(f"Loading {args.input}...")
    records = load_records(args.input)
    print(f"  Parsed {len(records)} valid rows.")

    groups = group_records(records, args.by_dataset)
    print(f"  Found {len(groups)} groups.\n")

    def sort_key(k):
        if args.by_dataset:
            dataset, det, tn, dt = k
            return (dataset, det, int(tn) if tn.isdigit() else 0, dt)
        else:
            det, tn, dt = k
            return (det, int(tn) if tn.isdigit() else 0, dt)

    sorted_keys = sorted(groups.keys(), key=sort_key)
    output_rows = print_table(sorted_keys, groups, args.by_dataset)

    fieldnames = list(output_rows[0].keys())
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"\nStats written to {args.output}")


if __name__ == "__main__":
    main()