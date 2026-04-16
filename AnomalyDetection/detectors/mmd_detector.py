import logging
import time
from typing import Optional

import numpy as np

from detectors.base_detector import BaseDetector
import config

log = logging.getLogger("detector.mmd")


class MMDDetector(BaseDetector):

    def __init__(self,
                 threshold: float = config.MMD_THRESHOLD,
                 sample_size: int = config.MMD_SAMPLE_SIZE):
        self.threshold   = threshold
        self.sample_size = sample_size
        self._calibrated = False

    @property
    def name(self) -> str:
        return "mmd"

    def _calibrate(self, reference: np.ndarray) -> None:
        n   = len(reference)
        idx = np.random.permutation(n)
        a   = reference[idx[: n // 2]]
        b   = reference[idx[n // 2 :]]

        null_score = self._mmd_rbf(
            self._sample(a, self.sample_size),
            self._sample(b, self.sample_size),
        )
        self.threshold = max(null_score * 5, config.MMD_THRESHOLD)
        self._calibrated = True
        log.info(
            "MMD threshold calibrated from reference: %.4f (null score=%.4f × 5)",
            self.threshold, null_score,
        )

    def update(self, current: np.ndarray, reference: np.ndarray) -> Optional[dict]:
        try:
            if len(current) < 10 or len(reference) < 20:
                return None

            if not self._calibrated:
                self._calibrate(reference)

            x = self._sample(current, self.sample_size)
            y = self._sample(reference, self.sample_size)

            score = self._mmd_rbf(x, y)
            log.info("mmd  score=%.4f  threshold=%.4f", score, self.threshold)

            if score > self.threshold:
                return {
                    "detector":    self.name,
                    "score":       float(score),
                    "threshold":   self.threshold,
                    "detected_at": int(time.time() * 1000),
                }
        except Exception as e:
            log.warning("MMD error: %s", e)
        return None

    def _mmd_rbf(self, x: np.ndarray, y: np.ndarray) -> float:
        xy = np.vstack([x, y])
        dists = np.sum((xy[:, None] - xy[None, :]) ** 2, axis=-1)
        base_sigma2 = float(np.median(dists[dists > 0]))
        if base_sigma2 == 0:
            return 0.0

        bandwidths = [base_sigma2 * s for s in (0.1, 0.5, 1.0, 2.0, 10.0)]

        n, m = len(x), len(y)
        mmd2 = 0.0

        for sigma2 in bandwidths:
            def rbf(a, b, s=sigma2):
                d = np.sum((a[:, None] - b[None, :]) ** 2, axis=-1)
                return np.exp(-d / (2 * s))

            kxx = rbf(x, x)
            kyy = rbf(y, y)
            kxy = rbf(x, y)

            np.fill_diagonal(kxx, 0)
            np.fill_diagonal(kyy, 0)

            mmd2 += (kxx.sum() / (n * (n - 1))
                     + kyy.sum() / (m * (m - 1))
                     - 2 * kxy.mean())

        return float(max(mmd2, 0.0)) ** 0.5

    def _sample(self, arr: np.ndarray, n: int) -> np.ndarray:
        if len(arr) <= n:
            return arr
        idx = np.random.choice(len(arr), n, replace=False)
        return arr[idx]