import logging
import time
from typing import Optional

import numpy as np
from scipy import stats

from detectors.base_detector import BaseDetector
import config

log = logging.getLogger("detector.padd")


# implementation of projection-based drift detector
class PADDDetector(BaseDetector):


    def __init__(self,
                 hidden_dim: int = config.PADD_HIDDEN_DIM,
                 alpha: float = config.PADD_ALPHA,
                 theta: float = config.PADD_THRESHOLD,
                 replications: int = config.PADD_REPLICATIONS,
                 sample_size: int = config.PADD_SAMPLE_SIZE,
                 seed: int = 42):
        self.hidden_dim = hidden_dim
        self.alpha = alpha
        self.theta = theta
        self.replications = replications
        self.sample_size = sample_size
        self.seed = seed

        self._weights: Optional[np.ndarray] = None
        self._bias: Optional[np.ndarray] = None
        self._rng = np.random.default_rng(seed)

    @property
    def name(self) -> str:
        return "padd"

    def _init_network(self, input_dim: int):
        self._weights = self._rng.normal(0, 0.1, (input_dim, self.hidden_dim))
        self._bias = self._rng.normal(0, 0.1, (self.hidden_dim,))
        log.info("PADD initialized: input_dim=%d, hidden_dim=%d, seed=%d", input_dim, self.hidden_dim, self.seed)

    def _get_activations(self, x: np.ndarray) -> np.ndarray:
        z = np.dot(x, self._weights) + self._bias
        return 1.0 / (1.0 + np.exp(-z))

    def update(self, current: np.ndarray, reference: np.ndarray) -> Optional[dict]:
        try:
            if len(current) < 10 or len(reference) < self.sample_size:
                return None

            if self._weights is None:
                self._init_network(current.shape[1])

            h_ref = self._get_activations(reference)
            h_cur = self._get_activations(current)

            rejections = 0
            for _ in range(self.replications):
                s_ref = self._sample(h_ref, self.sample_size)
                s_cur = self._sample(h_cur, self.sample_size)
                
                t_stat, p_val = stats.ttest_ind(s_ref.mean(axis=1), s_cur.mean(axis=1), equal_var=False)
                if p_val < self.alpha:
                    rejections += 1

            rejection_rate = rejections / self.replications
            log.info("PADD rejections: %d/%d (rate: %.2f, threshold: %.2f)", 
                     rejections, self.replications, rejection_rate, self.theta)

            if rejection_rate > self.theta:
                return {
                    "detector":    self.name,
                    "score":       float(rejection_rate),
                    "threshold":   self.theta,
                    "detected_at": int(time.time() * 1000),
                }

        except Exception as e:
            log.warning("PADD error: %s", e)
        return None

    def _sample(self, arr: np.ndarray, n: int) -> np.ndarray:
        if len(arr) <= n:
            return arr
        idx = self._rng.choice(len(arr), n, replace=False)
        return arr[idx]
