import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import os

matplotlib.rcParams.update({
    'font.size': 12,
    'font.family': 'serif',
    'axes.titlesize': 14,
    'axes.labelsize': 12,
    'xtick.labelsize': 11,
    'ytick.labelsize': 11,
    'legend.fontsize': 10,
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
})

OUTPUT_DIR = 'charts'
os.makedirs(OUTPUT_DIR, exist_ok=True)

df = pd.read_csv(
    'evaluation_results.csv',
    names=['Dataset','Detector','WindowType','TriggerN','DriftType',
           'Precision','Recall','F1','LatencyDocs','LatencyMs',
           'FAR','D1','D2','R','R_matchaware'],
    skiprows=1
)

# Filter out __GLOBAL__ for per-drift-type charts
df_drift = df[df['DriftType'] != '__GLOBAL__'].copy()
df_global = df[df['DriftType'] == '__GLOBAL__'].copy()

COLORS = {
    'mmd': '#1D9E75',
    'padd': '#7F77DD',
}

DATASET_LABELS = {
    'newsgroups': '20 Newsgroups',
    'yahoo': 'Yahoo Answers',
    'agnews': 'AG News',
}

DATASET_ORDER = ['newsgroups', 'yahoo', 'agnews']
DRIFT_ORDER = ['sudden', 'gradual', 'recurring']

def chart_cross_dataset_f1():
    data = df_drift[df_drift['TriggerN'] == 100]
    
    fig, axes = plt.subplots(1, 3, figsize=(14, 5), sharey=True)
    
    for ax, drift in zip(axes, DRIFT_ORDER):
        subset = data[data['DriftType'] == drift]
        
        x = np.arange(len(DATASET_ORDER))
        width = 0.35
        
        for i, det in enumerate(['mmd', 'padd']):
            means = []
            stds = []
            for ds in DATASET_ORDER:
                vals = subset[(subset['Dataset'] == ds) & (subset['Detector'] == det)]['F1']
                means.append(vals.mean())
                stds.append(vals.std())
            
            offset = -width/2 + i * width
            bars = ax.bar(x + offset, means, width, yerr=[s/np.sqrt(10) for s in stds],
                         label=det.upper(), color=COLORS[det],
                         edgecolor='white', linewidth=0.5,
                         capsize=3, error_kw={'linewidth': 1})
        
        ax.set_title(f'{drift.capitalize()} drift')
        ax.set_xticks(x)
        ax.set_xticklabels([DATASET_LABELS[d] for d in DATASET_ORDER], )
        ax.set_ylim(0, 1.15)
        ax.set_ylabel('F1 score')
        ax.legend(loc='upper right')
        ax.grid(axis='y', alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        if drift == 'recurring':
            ax.text(0.5, 0.5, 'No successful detections\n(F1 = 0)',
                    ha='center', va='center', fontsize=12, color='gray',
                    style='italic', transform=ax.transAxes)
    
    fig.suptitle('Cross-dataset F1 comparison (TRIGGER_N = 100)', fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/cross_dataset_f1.png')
    plt.close()
    print('Saved: cross_dataset_f1.png')

def chart_ablation_f1():
    data = df_drift[df_drift['Dataset'] == 'newsgroups']
    
    fig, axes = plt.subplots(1, 2, figsize=(10, 5), sharey=True)
    trigger_values = [50, 100, 200]
    
    for ax, drift in zip(axes, ['sudden', 'gradual']):
        subset = data[data['DriftType'] == drift]
        
        x = np.arange(len(trigger_values))
        width = 0.35
        
        for i, det in enumerate(['mmd', 'padd']):
            means = []
            stds = []
            for tn in trigger_values:
                vals = subset[(subset['TriggerN'] == tn) & (subset['Detector'] == det)]['F1']
                means.append(vals.mean())
                stds.append(vals.std())
            
            offset = -width/2 + i * width
            ax.bar(x + offset, means, width, yerr=[s/np.sqrt(10) for s in stds],
                   label=det.upper(), color=COLORS[det],
                   edgecolor='white', linewidth=0.5,
                   capsize=3, error_kw={'linewidth': 1})
        
        ax.set_title(f'{drift.capitalize()} drift')
        ax.set_xticks(x)
        ax.set_xticklabels([f'N={n}' for n in trigger_values])
        ax.set_ylim(0, 1.15)
        ax.set_ylabel('F1 score')
        ax.legend(loc='upper left')
        ax.grid(axis='y', alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
    
    fig.suptitle('TRIGGER_N ablation on 20 Newsgroups', fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/ablation_f1.png')
    plt.close()
    print('Saved: ablation_f1.png')

def chart_ablation_latency():
    data = df_drift[(df_drift['Dataset'] == 'newsgroups') & 
                     (df_drift['DriftType'].isin(['sudden', 'gradual']))]
    
    fig, axes = plt.subplots(1, 2, figsize=(10, 5), sharey=True)
    trigger_values = [50, 100, 200]
    
    for ax, drift in zip(axes, ['sudden', 'gradual']):
        subset = data[data['DriftType'] == drift]
        
        x = np.arange(len(trigger_values))
        width = 0.35
        
        for i, det in enumerate(['mmd', 'padd']):
            means = []
            stds = []
            for tn in trigger_values:
                vals = subset[(subset['TriggerN'] == tn) & (subset['Detector'] == det)]['LatencyDocs'].dropna()
                means.append(vals.mean() if len(vals) > 0 else 0)
                stds.append(vals.std() if len(vals) > 1 else 0)
            
            offset = -width/2 + i * width
            ax.bar(x + offset, means, width, yerr=[s/np.sqrt(10) for s in stds],
                   label=det.upper(), color=COLORS[det],
                   edgecolor='white', linewidth=0.5,
                   capsize=3, error_kw={'linewidth': 1})
        
        ax.set_title(f'{drift.capitalize()} drift')
        ax.set_xticks(x)
        ax.set_xticklabels([f'N={n}' for n in trigger_values])
        ax.set_ylabel('Latency (documents)')
        ax.legend(loc='upper left')
        ax.grid(axis='y', alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
    
    fig.suptitle('Detection latency by TRIGGER_N on 20 Newsgroups', fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/ablation_latency.png')
    plt.close()
    print('Saved: ablation_latency.png')

def chart_padd_gradient():
    data = df_drift[(df_drift['TriggerN'] == 100) & (df_drift['Detector'] == 'padd')]
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    x = np.arange(len(DATASET_ORDER))
    width = 0.35
    
    for i, drift in enumerate(['sudden', 'gradual']):
        subset = data[data['DriftType'] == drift]
        means = []
        stds = []
        for ds in DATASET_ORDER:
            vals = subset[subset['Dataset'] == ds]['F1']
            means.append(vals.mean())
            stds.append(vals.std())
        
        offset = -width/2 + i * width
        color = '#7F77DD' if drift == 'sudden' else '#AFA9EC'
        ax.bar(x + offset, means, width, yerr=[s/np.sqrt(10) for s in stds],
               label=f'{drift.capitalize()} drift', color=color,
               edgecolor='white', linewidth=0.5,
               capsize=3, error_kw={'linewidth': 1})
    
    text_lengths = ['200-500+ words', '50-200 words', '~10 words']
    for i, txt in enumerate(text_lengths):
        ax.annotate(txt, (i, -0.08), ha='center', fontsize=9, 
                   color='gray', style='italic')
    
    ax.set_xticks(x)
    ax.set_xticklabels([DATASET_LABELS[d] for d in DATASET_ORDER])
    ax.set_ylim(0, 1.15)
    ax.set_ylabel('F1 score')
    ax.set_title('PADD performance by dataset (TRIGGER_N = 100)')
    ax.legend(loc='upper right')
    ax.grid(axis='y', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/padd_gradient.png')
    plt.close()
    print('Saved: padd_gradient.png')

def chart_per_seed():
    data = df_drift[(df_drift['Dataset'] == 'newsgroups') & 
                     (df_drift['TriggerN'] == 100) &
                     (df_drift['DriftType'].isin(['sudden', 'gradual']))]
    
    fig, axes = plt.subplots(1, 2, figsize=(10, 5), sharey=True)
    
    for ax, drift in zip(axes, ['sudden', 'gradual']):
        subset = data[data['DriftType'] == drift]
        
        for i, det in enumerate(['mmd', 'padd']):
            vals = subset[subset['Detector'] == det]['F1'].values
            jitter = np.random.RandomState(42).uniform(-0.12, 0.12, len(vals))
            ax.scatter(np.full_like(vals, i) + jitter, vals, 
                      color=COLORS[det], alpha=0.6, s=40, edgecolors='white', linewidth=0.5)
            ax.hlines(vals.mean(), i - 0.25, i + 0.25, colors=COLORS[det], linewidth=2)
        
        ax.set_title(f'{drift.capitalize()} drift')
        ax.set_xticks([0, 1])
        ax.set_xticklabels(['MMD', 'PADD'])
        ax.set_ylim(-0.05, 1.15)
        ax.set_ylabel('F1 score')
        ax.grid(axis='y', alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
    
    fig.suptitle('Per-seed F1 on 20 Newsgroups (TRIGGER_N = 100)', fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/per_seed_f1.png')
    plt.close()
    print('Saved: per_seed_f1.png')

def chart_far():
    data = df_drift[(df_drift['TriggerN'] == 100) & (df_drift['DriftType'] == 'sudden')]
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    x = np.arange(len(DATASET_ORDER))
    width = 0.35
    
    for i, det in enumerate(['mmd', 'padd']):
        means = []
        stds = []
        for ds in DATASET_ORDER:
            vals = data[(data['Dataset'] == ds) & (data['Detector'] == det)]['FAR']
            means.append(vals.mean())
            stds.append(vals.std())
        
        offset = -width/2 + i * width
        ax.bar(x + offset, means, width, yerr=[s/np.sqrt(10) for s in stds],
               label=det.upper(), color=COLORS[det],
               edgecolor='white', linewidth=0.5,
               capsize=3, error_kw={'linewidth': 1})
    
    ax.set_xticks(x)
    ax.set_xticklabels([DATASET_LABELS[d] for d in DATASET_ORDER])
    ax.set_ylabel('False Alarm Rate')
    ax.set_title('False Alarm Rate by dataset (TRIGGER_N = 100)')
    ax.legend(loc='upper right')
    ax.grid(axis='y', alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/far_comparison.png')
    plt.close()
    print('Saved: far_comparison.png')

def summary_tables():
    data = df_drift[df_drift['TriggerN'] == 100]
    summary = data.groupby(['Dataset', 'Detector', 'DriftType']).agg(
        F1_mean=('F1', 'mean'),
        F1_std=('F1', 'std'),
        Latency_mean=('LatencyDocs', 'mean'),
        FAR_mean=('FAR', 'mean'),
    ).round(4).reset_index()
    summary.to_csv(f'{OUTPUT_DIR}/summary_cross_dataset.csv', index=False)
    print('Saved: summary_cross_dataset.csv')
    
    data = df_drift[df_drift['Dataset'] == 'newsgroups']
    summary = data.groupby(['TriggerN', 'Detector', 'DriftType']).agg(
        F1_mean=('F1', 'mean'),
        F1_std=('F1', 'std'),
        Latency_mean=('LatencyDocs', 'mean'),
        FAR_mean=('FAR', 'mean'),
    ).round(4).reset_index()
    summary.to_csv(f'{OUTPUT_DIR}/summary_ablation.csv', index=False)
    print('Saved: summary_ablation.csv')


if __name__ == '__main__':
    print('Generating charts...\n')
    chart_cross_dataset_f1()
    chart_ablation_f1()
    chart_ablation_latency()
    chart_padd_gradient()
    chart_per_seed()
    chart_far()
    summary_tables()
    print('\nAll charts saved to', OUTPUT_DIR)