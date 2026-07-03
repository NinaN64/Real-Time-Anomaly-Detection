import numpy as np
from scipy import stats
import time

def user_padd(h_ref, h_cur, hidden_dim, replications, sample_size, alpha, rng):
    idx_ref = rng.choice(len(h_ref), size=(hidden_dim, replications, sample_size), replace=True)
    idx_cur = rng.choice(len(h_cur), size=(hidden_dim, replications, sample_size), replace=True)
    
    neuron_idx = np.arange(hidden_dim)[:, None, None]
    s_ref = h_ref[idx_ref, neuron_idx]
    s_cur = h_cur[idx_cur, neuron_idx]
    
    _, p_values = stats.ttest_ind(s_ref, s_cur, axis=-1, equal_var=False)
    rejections = int(np.sum(p_values < alpha))
    return rejections

def optimized_padd(h_ref, h_cur, hidden_dim, replications, sample_size, alpha, rng, t_crit):
    idx_ref = rng.choice(len(h_ref), size=(hidden_dim, replications, sample_size), replace=True)
    idx_cur = rng.choice(len(h_cur), size=(hidden_dim, replications, sample_size), replace=True)
    
    neuron_idx = np.arange(hidden_dim)[:, None, None]
    s_ref = h_ref[idx_ref, neuron_idx]
    s_cur = h_cur[idx_cur, neuron_idx]
    
    mean_ref = np.mean(s_ref, axis=-1)
    mean_cur = np.mean(s_cur, axis=-1)
    
    var_ref = np.var(s_ref, axis=-1, ddof=1)
    var_cur = np.var(s_cur, axis=-1, ddof=1)
    
    numerator = mean_ref - mean_cur
    denominator = np.sqrt((var_ref + var_cur) / sample_size)
    
    t_stat = np.zeros_like(numerator)
    nonzero = denominator > 1e-9
    t_stat[nonzero] = numerator[nonzero] / denominator[nonzero]
    
    rejections = np.sum(np.abs(t_stat) > t_crit)
    return rejections

def test_timing():
    hidden_dim = 32
    replications = 12
    sample_size = 50
    alpha = 0.07
    
    ref = np.random.normal(0, 1, (200, hidden_dim))
    cur = np.random.normal(0.1, 1, (100, hidden_dim))
    
    df = 2 * sample_size - 2
    t_crit = stats.t.ppf(1 - alpha / 2, df)
    
    # User's new PADD
    rng1 = np.random.default_rng(42)
    start = time.time()
    for _ in range(100):
        rejections_user = user_padd(ref, cur, hidden_dim, replications, sample_size, alpha, rng1)
    user_time = time.time() - start
    print(f"User PADD (100 runs): took {user_time:.4f} seconds ({user_time/100:.6f} s/run)")
    
    # Optimized PADD (Vectorized arithmetic)
    rng2 = np.random.default_rng(42)
    start = time.time()
    for _ in range(100):
        rejections_opt = optimized_padd(ref, cur, hidden_dim, replications, sample_size, alpha, rng2, t_crit)
    opt_time = time.time() - start
    print(f"Optimized PADD (100 runs): took {opt_time:.4f} seconds ({opt_time/100:.6f} s/run)")
    
    print(f"Speedup: {user_time / opt_time:.1f}x")

if __name__ == "__main__":
    test_timing()
