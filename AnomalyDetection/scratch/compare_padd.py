import numpy as np
from scipy import stats
import time

# --- Current Implementation ---
class CurrentPADDDetector:
    def __init__(self, hidden_dim=32, alpha=0.07, theta=0.20, replications=12, sample_size=50, seed=42):
        self.hidden_dim = hidden_dim
        self.alpha = alpha
        self.theta = theta
        self.replications = replications
        self.sample_size = sample_size
        self.seed = seed
        self._weights = None
        self._bias = None
        self._rng = np.random.default_rng(seed)

    def _init_network(self, input_dim):
        self._weights = self._rng.normal(0, 0.1, (input_dim, self.hidden_dim))
        self._bias = self._rng.normal(0, 0.1, (self.hidden_dim,))

    def _get_activations(self, x):
        z = x @ self._weights + self._bias
        return np.maximum(0, z)

    def _sample(self, arr, n):
        idx = self._rng.choice(len(arr), size=n, replace=True)
        return arr[idx]

    def update(self, current, reference):
        if len(current) < 10 or len(reference) < self.sample_size:
            return None
        if self._weights is None:
            self._init_network(current.shape[1])
        h_ref = self._get_activations(reference)
        h_cur = self._get_activations(current)
        rejections = 0
        for neuron in range(self.hidden_dim):
            ref_dim = h_ref[:, neuron]
            cur_dim = h_cur[:, neuron]
            for _ in range(self.replications):
                s_ref = self._sample(ref_dim, self.sample_size)
                s_cur = self._sample(cur_dim, self.sample_size)
                _, p = stats.ttest_ind(s_ref, s_cur, equal_var=False)
                if p < self.alpha:
                    rejections += 1
        total_tests = self.hidden_dim * self.replications
        rejection_rate = rejections / total_tests
        threshold_count = self.theta * total_tests
        if rejections > threshold_count:
            return {"score": rejection_rate}
        return None

# --- Corrected (Official Paper style) Implementation ---
class CorrectedPADDDetector:
    def __init__(self, ensemble_size=30, neck_width=10, alpha=0.05, theta=0.17, replications=35, sample_size=75, seed=42):
        self.ensemble_size = ensemble_size
        self.neck_width = neck_width
        self.alpha = alpha
        self.theta = theta
        self.replications = replications
        self.sample_size = sample_size
        self.seed = seed
        self.stack = None
        self._rng = np.random.default_rng(seed)

    def _init_network(self, input_dim):
        # We draw from normal distribution as in paper
        self.stack = [
            self._rng.normal(0, 0.1, (input_dim + 1, self.neck_width)),
            self._rng.normal(0, 0.1, (self.neck_width, self.ensemble_size)),
        ]

    def _predict_proba(self, x):
        val = np.concatenate((np.copy(x), np.ones((x.shape[0], 1))), axis=1)
        # Hidden layer
        val = np.maximum(0, val @ self.stack[0])
        # Output layer
        val = np.maximum(0, val @ self.stack[1])
        # Softmax
        predict_proba = np.exp(val - np.max(val, axis=1)[:, None])
        predict_proba = predict_proba / np.sum(predict_proba, axis=1)[:, None]
        return predict_proba

    def _sample(self, arr, n):
        idx = self._rng.choice(len(arr), size=n, replace=True)
        return arr[idx]

    def update(self, current, reference):
        if len(current) < 10 or len(reference) < self.sample_size:
            return None
        if self.stack is None:
            self._init_network(current.shape[1])
        
        h_ref = self._predict_proba(reference)
        h_cur = self._predict_proba(current)
        
        rejections = 0
        for neuron in range(self.ensemble_size):
            ref_dim = h_ref[:, neuron]
            cur_dim = h_cur[:, neuron]
            for _ in range(self.replications):
                s_ref = self._sample(ref_dim, self.sample_size)
                s_cur = self._sample(cur_dim, self.sample_size)
                _, p = stats.ttest_ind(s_ref, s_cur, equal_var=True) # Paper uses standard t-test
                if p < self.alpha:
                    rejections += 1
        
        total_tests = self.ensemble_size * self.replications
        rejection_rate = rejections / total_tests
        threshold_count = self.theta * total_tests
        if rejections > threshold_count:
            return {"score": rejection_rate}
        return None

# --- Run simulation ---
def run_simulation():
    input_dim = 384
    ref_size = 200
    cur_size = 100
    n_trials = 100
    
    print(f"Running {n_trials} trials under NO DRIFT...")
    
    current_alerts = 0
    corrected_alerts = 0
    
    current_scores = []
    corrected_scores = []
    
    # We generate a common random generator to be fair
    rng = np.random.default_rng(12345)
    
    for i in range(n_trials):
        # Baseline data
        reference = rng.normal(0, 1, (ref_size, input_dim))
        current = rng.normal(0, 1, (cur_size, input_dim))
        
        # Current
        det_curr = CurrentPADDDetector(seed=i)
        res_curr = det_curr.update(current, reference)
        if res_curr:
            current_alerts += 1
            current_scores.append(res_curr["score"])
        
        # Corrected
        det_corr = CorrectedPADDDetector(seed=i)
        res_corr = det_corr.update(current, reference)
        if res_corr:
            corrected_alerts += 1
            corrected_scores.append(res_corr["score"])
            
    print(f"Current PADD False Alarms: {current_alerts}/{n_trials} ({current_alerts/n_trials*100:.1f}%)")
    print(f"Corrected PADD False Alarms: {corrected_alerts}/{n_trials} ({corrected_alerts/n_trials*100:.1f}%)")

if __name__ == "__main__":
    run_simulation()
