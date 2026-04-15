import logging
import time
from typing import Optional

import numpy as np
from sklearn.ensemble import IsolationForest

from detectors.base_detector import BaseDetector
import config

log = logging.getLogger("detector.isolation_forest")


class IsolationForestDetector(BaseDetector):

    def __init__(self,
                 contamination: float = config.IF_CONTAMINATION,
                 n_estimators: int = config.IF_N_ESTIMATORS,
                 threshold: float = config.IF_THRESHOLD):
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.threshold = threshold
        self._model: Optional[IsolationForest] = None
        self._last_ref_size = 0

    @property
    def name(self) -> str:
        return "isolation_forest"

    def update(self, current: np.ndarray, reference: np.ndarray) -> Optional[dict]:
        try:
            if len(current) < 5 or len(reference) < 20:
                return None

            if self._model is None or abs(len(reference) - self._last_ref_size) > len(reference) * 0.1:
                self._fit(reference)

            scores = self._model.score_samples(current)
            mean_score = float(scores.mean())

            log.debug("IF mean score: %.4f (threshold: %.4f)", mean_score, self.threshold)

            if mean_score < self.threshold:
                log.info("Isolation Forest drift detected! mean_score=%.4f", mean_score)
                return {
                    "detector":    self.name,
                    "score":       mean_score,
                    "threshold":   self.threshold,
                    "detected_at": int(time.time() * 1000),
                }
        except Exception as e:
            log.warning("IsolationForest error: %s", e)
        return None

    def _fit(self, reference: np.ndarray) -> None:
        log.debug("Fitting Isolation Forest on %d reference embeddings", len(reference))
        self._model = IsolationForest(
            n_estimators=self.n_estimators,
            contamination=self.contamination,
            random_state=42,
            n_jobs=-1,
        )
        self._model.fit(reference)
        self._last_ref_size = len(reference)