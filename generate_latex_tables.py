import os
import math
import numpy as np
import pandas as pd

OUT = "charts"
os.makedirs(OUT, exist_ok=True)

cross = pd.read_csv(os.path.join(OUT, "summary_cross_dataset_full.csv"))
ablat = pd.read_csv(os.path.join(OUT, "summary_ablation_full.csv"))

DATASET_ORDER = ["20 Newsgroups", "Yahoo Answers", "AG News"]
DRIFT_ORDER   = ["Sudden", "Gradual", "Recurring"]
N_ORDER       = [50, 100, 200]

def fmt(val, decimals=3):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return r"---"
    return f"{val:.{decimals}f}"


def fmt_pm(mean, std, decimals=3, bold=False):
    m = fmt(mean, decimals)
    s = fmt(std, decimals)
    cell = f"{m} $\\pm$ {s}"
    return f"\\textbf{{{cell}}}" if bold else cell


def write_tex(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"   -> {os.path.basename(path)}")

def table_cross_dataset_f1():
    print("[Table 1] Cross-dataset F1 ...")
    lines = []
    lines.append(r"% Table: Cross-dataset F1 comparison (Trigger N = 100)")
    lines.append(r"% Requires: \usepackage{booktabs}, \usepackage{multirow}")
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"  \centering")
    lines.append(r"  \caption{Mean F1 score ($\pm$ std, $n=10$ seeds) across datasets and")
    lines.append(r"           drift types for MMD and PADD detectors (Trigger $N=100$).}")
    lines.append(r"  \label{tab:cross_dataset_f1}")
    lines.append(r"  \begin{tabular}{llcc}")
    lines.append(r"    \toprule")
    lines.append(r"    \textbf{Dataset} & \textbf{Drift Type}")
    lines.append(r"      & \textbf{MMD F1} & \textbf{PADD F1} \\")
    lines.append(r"    \midrule")

    for di, ds in enumerate(DATASET_ORDER):
        sub = cross[cross["Dataset"] == ds]
        n_rows = len(DRIFT_ORDER)
        for ki, drift in enumerate(DRIFT_ORDER):
            row = sub[sub["DriftType"] == drift]
            if row.empty:
                mmd_cell = padd_cell = r"---"
            else:
                r = row.iloc[0]
                mmd_f1  = cross[(cross["Dataset"]==ds)&(cross["DriftType"]==drift)&(cross["Detector"]=="MMD")]
                padd_f1 = cross[(cross["Dataset"]==ds)&(cross["DriftType"]==drift)&(cross["Detector"]=="PADD")]
                if mmd_f1.empty or padd_f1.empty:
                    mmd_cell = padd_cell = r"---"
                else:
                    mv = mmd_f1.iloc[0];  pv = padd_f1.iloc[0]
                    mmd_bold  = mv["F1_mean"] > pv["F1_mean"]
                    padd_bold = pv["F1_mean"] > mv["F1_mean"]
                    mmd_cell  = fmt_pm(mv["F1_mean"],  mv["F1_std"],  bold=mmd_bold)
                    padd_cell = fmt_pm(pv["F1_mean"],  pv["F1_std"],  bold=padd_bold)

            if ki == 0:
                ds_col = rf"    \multirow{{{n_rows}}}{{*}}{{{ds}}}"
            else:
                ds_col = r"    "

            lines.append(f"{ds_col} & {drift} & {mmd_cell} & {padd_cell} \\\\")

        if di < len(DATASET_ORDER) - 1:
            lines.append(r"    \midrule")

    lines.append(r"    \bottomrule")
    lines.append(r"  \end{tabular}")
    lines.append(r"\end{table}")

    write_tex(os.path.join(OUT, "table_cross_dataset_f1.tex"), "\n".join(lines) + "\n")

def table_cross_dataset_lat_far():
    print("[Table 2] Cross-dataset Latency & FAR ...")
    drifts = ["Sudden", "Gradual"]
    lines = []
    lines.append(r"% Table: Cross-dataset detection latency and FAR (Trigger N = 100)")
    lines.append(r"% Requires: \usepackage{booktabs}, \usepackage{multirow}")
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"  \centering")
    lines.append(r"  \caption{Mean detection latency (documents) and false alarm rate (FAR)")
    lines.append(r"           ($\pm$ std, $n=10$ seeds) for Trigger $N=100$.")
    lines.append(r"           Lower latency and lower FAR are better.")
    lines.append(r"           Dashes indicate no detections (latency undefined).}")
    lines.append(r"  \label{tab:cross_dataset_lat_far}")
    lines.append(r"  \begin{tabular}{llcccc}")
    lines.append(r"    \toprule")
    lines.append(r"    \multirow{2}{*}{\textbf{Dataset}}")
    lines.append(r"      & \multirow{2}{*}{\textbf{Drift}}")
    lines.append(r"      & \multicolumn{2}{c}{\textbf{Latency (docs)}}")
    lines.append(r"      & \multicolumn{2}{c}{\textbf{FAR}} \\")
    lines.append(r"    \cmidrule(lr){3-4} \cmidrule(lr){5-6}")
    lines.append(r"      & & \textbf{MMD} & \textbf{PADD}")
    lines.append(r"        & \textbf{MMD} & \textbf{PADD} \\")
    lines.append(r"    \midrule")

    for di, ds in enumerate(DATASET_ORDER):
        sub = cross[cross["Dataset"] == ds]
        for ki, drift in enumerate(drifts):
            mmd_row  = sub[(sub["DriftType"]==drift) & (sub["Detector"]=="MMD")]
            padd_row = sub[(sub["DriftType"]==drift) & (sub["Detector"]=="PADD")]

            if mmd_row.empty or padd_row.empty:
                lat_mmd = lat_padd = far_mmd = far_padd = r"---"
            else:
                mv = mmd_row.iloc[0]; pv = padd_row.iloc[0]
                mv_lat = mv["Latency_mean"]; pv_lat = pv["Latency_mean"]
                mmd_lat_bold  = (not (isinstance(mv_lat, float) and math.isnan(mv_lat))) and \
                                ((isinstance(pv_lat, float) and math.isnan(pv_lat)) or
                                 mv_lat <= pv_lat)
                padd_lat_bold = (not (isinstance(pv_lat, float) and math.isnan(pv_lat))) and \
                                ((isinstance(mv_lat, float) and math.isnan(mv_lat)) or
                                 pv_lat < mv_lat)
                lat_mmd  = fmt_pm(mv["Latency_mean"], mv["Latency_std"], decimals=1, bold=mmd_lat_bold)
                lat_padd = fmt_pm(pv["Latency_mean"], pv["Latency_std"], decimals=1, bold=padd_lat_bold)
                mmd_far_bold  = mv["FAR_mean"] <= pv["FAR_mean"]
                padd_far_bold = pv["FAR_mean"] < mv["FAR_mean"]
                far_mmd  = fmt_pm(mv["FAR_mean"], mv["FAR_std"], decimals=4, bold=mmd_far_bold)
                far_padd = fmt_pm(pv["FAR_mean"], pv["FAR_std"], decimals=4, bold=padd_far_bold)

            if ki == 0:
                ds_col = rf"    \multirow{{2}}{{*}}{{{ds}}}"
            else:
                ds_col = r"    "

            lines.append(f"{ds_col} & {drift} & {lat_mmd} & {lat_padd} & {far_mmd} & {far_padd} \\\\")

        if di < len(DATASET_ORDER) - 1:
            lines.append(r"    \midrule")

    lines.append(r"    \bottomrule")
    lines.append(r"  \end{tabular}")
    lines.append(r"\end{table}")

    write_tex(os.path.join(OUT, "table_cross_dataset_lat_far.tex"), "\n".join(lines) + "\n")

def table_ablation_f1():
    print("[Table 3] Ablation F1 ...")
    drifts = ["Sudden", "Gradual"]
    lines = []
    lines.append(r"% Table: Trigger-N ablation F1 (20 Newsgroups)")
    lines.append(r"% Requires: \usepackage{booktabs}, \usepackage{multirow}")
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"  \centering")
    lines.append(r"  \caption{Mean F1 score ($\pm$ std, $n=10$ seeds) for the Trigger-$N$ ablation")
    lines.append(r"           study on the 20~Newsgroups dataset.")
    lines.append(r"           Bold values indicate the better detector per row.}")
    lines.append(r"  \label{tab:ablation_f1}")
    lines.append(r"  \begin{tabular}{llcc}")
    lines.append(r"    \toprule")
    lines.append(r"    \textbf{Trigger $N$} & \textbf{Drift Type}")
    lines.append(r"      & \textbf{MMD F1} & \textbf{PADD F1} \\")
    lines.append(r"    \midrule")

    for ni, n in enumerate(N_ORDER):
        sub = ablat[ablat["TriggerN"] == n]
        for ki, drift in enumerate(drifts):
            mmd_row  = sub[(sub["DriftType"]==drift) & (sub["Detector"]=="MMD")]
            padd_row = sub[(sub["DriftType"]==drift) & (sub["Detector"]=="PADD")]

            if mmd_row.empty or padd_row.empty:
                mmd_cell = padd_cell = r"---"
            else:
                mv = mmd_row.iloc[0]; pv = padd_row.iloc[0]
                mmd_bold  = mv["F1_mean"] >= pv["F1_mean"]
                padd_bold = pv["F1_mean"] > mv["F1_mean"]
                mmd_cell  = fmt_pm(mv["F1_mean"], mv["F1_std"], bold=mmd_bold)
                padd_cell = fmt_pm(pv["F1_mean"], pv["F1_std"], bold=padd_bold)

            n_label = rf"$N={n}$"
            if ki == 0:
                n_col = rf"    \multirow{{2}}{{*}}{{{n_label}}}"
            else:
                n_col = r"    "

            lines.append(f"{n_col} & {drift} & {mmd_cell} & {padd_cell} \\\\")

        if ni < len(N_ORDER) - 1:
            lines.append(r"    \midrule")

    lines.append(r"    \bottomrule")
    lines.append(r"  \end{tabular}")
    lines.append(r"\end{table}")

    write_tex(os.path.join(OUT, "table_ablation_f1.tex"), "\n".join(lines) + "\n")

def table_ablation_latency():
    print("[Table 4] Ablation latency ...")
    drifts = ["Sudden", "Gradual"]
    lines = []
    lines.append(r"% Table: Trigger-N ablation detection latency (20 Newsgroups)")
    lines.append(r"% Requires: \usepackage{booktabs}, \usepackage{multirow}")
    lines.append(r"\begin{table}[htbp]")
    lines.append(r"  \centering")
    lines.append(r"  \caption{Mean detection latency in documents ($\pm$ std) for the")
    lines.append(r"           Trigger-$N$ ablation on 20~Newsgroups.")
    lines.append(r"           Dashes indicate no true detections (latency undefined).")
    lines.append(r"           Bold marks the lower (faster) mean per row.}")
    lines.append(r"  \label{tab:ablation_latency}")
    lines.append(r"  \begin{tabular}{llcc}")
    lines.append(r"    \toprule")
    lines.append(r"    \textbf{Trigger $N$} & \textbf{Drift Type}")
    lines.append(r"      & \textbf{MMD Latency} & \textbf{PADD Latency} \\")
    lines.append(r"    \midrule")

    for ni, n in enumerate(N_ORDER):
        sub = ablat[ablat["TriggerN"] == n]
        for ki, drift in enumerate(drifts):
            mmd_row  = sub[(sub["DriftType"]==drift) & (sub["Detector"]=="MMD")]
            padd_row = sub[(sub["DriftType"]==drift) & (sub["Detector"]=="PADD")]

            if mmd_row.empty or padd_row.empty:
                mmd_cell = padd_cell = r"---"
            else:
                mv = mmd_row.iloc[0]; pv = padd_row.iloc[0]
                mv_l = mv["Latency_mean"]; pv_l = pv["Latency_mean"]
                mv_nan = isinstance(mv_l, float) and math.isnan(mv_l)
                pv_nan = isinstance(pv_l, float) and math.isnan(pv_l)
                mmd_bold  = (not mv_nan) and (pv_nan or mv_l <= pv_l)
                padd_bold = (not pv_nan) and (mv_nan or pv_l < mv_l)
                mmd_cell  = fmt_pm(mv_l, mv["Latency_std"], decimals=1, bold=mmd_bold)
                padd_cell = fmt_pm(pv_l, pv["Latency_std"], decimals=1, bold=padd_bold)

            n_label = rf"$N={n}$"
            if ki == 0:
                n_col = rf"    \multirow{{2}}{{*}}{{{n_label}}}"
            else:
                n_col = r"    "

            lines.append(f"{n_col} & {drift} & {mmd_cell} & {padd_cell} \\\\")

        if ni < len(N_ORDER) - 1:
            lines.append(r"    \midrule")

    lines.append(r"    \bottomrule")
    lines.append(r"  \end{tabular}")
    lines.append(r"\end{table}")

    write_tex(os.path.join(OUT, "table_ablation_latency.tex"), "\n".join(lines) + "\n")

if __name__ == "__main__":
    print("Generating LaTeX tables ...")
    table_cross_dataset_f1()
    table_cross_dataset_lat_far()
    table_ablation_f1()
    table_ablation_latency()
    print("\nDone! Files saved to charts/")
    for f in sorted(os.listdir(OUT)):
        if f.endswith(".tex"):
            sz = os.path.getsize(os.path.join(OUT, f))
            print(f"  {f:45s}  ({sz:>6,} bytes)")
