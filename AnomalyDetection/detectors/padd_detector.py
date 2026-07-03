import logging
import time
from typing import Optional

import numpy as np
from scipy import stats

from detectors.base_detector import BaseDetector
import config

log = logging.getLogger("detector.padd")


class PADDDetector(BaseDetector):
    """
    Parallel Activations Drift Detector (PADD).


    This implementation is inspired by the original PADD method and the
    reference implementation provided by Paweł Ksieniewicz et al.


    Paper:
    Komorniczak, J., Ksieniewicz, P.
    "Unsupervised Concept Drift Detection Based on Parallel Activations
    of Neural Network"


    Reference implementation:
    https://github.com/w4k2/padd


    Notes:
    This implementation was adapted for real-time textual stream processing
    using SBERT embeddings and Kafka-based windowed detection.
    """

    def __init__(
        self,
        hidden_dim: int = config.PADD_HIDDEN_DIM,
        neck_width: int = 10,
        alpha: float = config.PADD_ALPHA,
        theta: float = config.PADD_THRESHOLD,
        replications: int = config.PADD_REPLICATIONS,
        sample_size: int = config.PADD_SAMPLE_SIZE,
        init_scale: float = 0.1,
        max_history: int = 5000,
        seed: int = 42
    ):
        super().__init__()

        self.hidden_dim = hidden_dim
        self.neck_width = neck_width
        self.alpha = alpha
        self.theta = theta
        self.replications = replications
        self.sample_size = sample_size

        self.init_scale = init_scale
        self.max_history = max_history

        self.seed = seed

        self.network = None
        self._rng = np.random.default_rng(seed)
        self._past_activations: Optional[np.ndarray] = None

    @property
    def name(self) -> str:
        return "padd"

    def _init_network(
        self,
        input_dim: int
    ) -> None:

        self.network = [

            # Input -> bottleneck
            self._rng.normal(
                loc=0.0,
                scale=self.init_scale,
                size=(
                    input_dim + 1,
                    self.neck_width
                )
            ),

            # Bottleneck -> hidden
            self._rng.normal(
                loc=0.0,
                scale=self.init_scale,
                size=(
                    self.neck_width + 1,
                    self.hidden_dim
                )
            )
        ]

        log.info(
            (
                "PADD initialized: "
                "input=%d "
                "neck=%d "
                "hidden=%d "
                "scale=%.3f "
                "seed=%d"
            ),
            input_dim,
            self.neck_width,
            self.hidden_dim,
            self.init_scale,
            self.seed
        )

    def _get_activations(
        self,
        x: np.ndarray
    ) -> np.ndarray:

        # Add input bias
        x = np.column_stack(
            (x, np.ones(len(x)))
        )

        # Layer 1
        val = np.maximum(
            0,
            x @ self.network[0]
        )

        # Add hidden bias
        val = np.column_stack(
            (val, np.ones(len(val)))
        )

        # Layer 2
        val = np.maximum(
            0,
            val @ self.network[1]
        )

        return val

    def _reset_history(
        self,
        activations: np.ndarray
    ) -> None:

        self._past_activations = activations.copy()

    def _append_history(
        self,
        activations: np.ndarray
    ) -> None:

        self._past_activations = np.concatenate(
            (
                self._past_activations,
                activations
            ),
            axis=0
        )

        # Bound memory growth
        if len(self._past_activations) > self.max_history:

            self._past_activations = (
                self._past_activations[
                    -self.max_history:
                ]
            )

    def _run_test(
        self,
        h_ref: np.ndarray,
        h_cur: np.ndarray
    ) -> Optional[dict]:

        idx_ref = self._rng.choice(
            len(h_ref),
            size=(
                self.hidden_dim,
                self.replications,
                self.sample_size
            ),
            replace=True
        )

        idx_cur = self._rng.choice(
            len(h_cur),
            size=(
                self.hidden_dim,
                self.replications,
                self.sample_size
            ),
            replace=True
        )

        neuron_idx = np.arange(
            self.hidden_dim
        )[:, None, None]

        s_ref = h_ref[
            idx_ref,
            neuron_idx
        ]

        s_cur = h_cur[
            idx_cur,
            neuron_idx
        ]

        # Student's t-test following original PADD
        _, p_values = stats.ttest_ind(
            s_ref,
            s_cur,
            axis=-1,
            equal_var=True
        )

        p_values = np.nan_to_num(
            p_values,
            nan=1.0,
            posinf=1.0,
            neginf=1.0
        )

        rejections = int(
            np.sum(
                p_values < self.alpha
            )
        )

        total_tests = (
            self.hidden_dim *
            self.replications
        )

        rejection_rate = (
            rejections /
            total_tests
        )

        threshold_count = int(
            round(
                self.theta *
                total_tests
            )
        )

        log.info(
            "PADD: %d/%d rejections (%.3f)",
            rejections,
            total_tests,
            rejection_rate
        )

        if rejections > threshold_count:

            return {
                "detector": self.name,
                "score": float(
                    rejection_rate
                ),
                "threshold": self.theta,
                "detected_at": int(
                    time.time() * 1000
                )
            }

        return None

    def update(
        self,
        current: np.ndarray,
        reference: np.ndarray
    ) -> Optional[dict]:

        try:

            if current is None or len(current) == 0:
                return None

            if self.network is None:

                self._init_network(
                    current.shape[1]
                )

            h_cur = self._get_activations(
                current
            )

            # Diagnostic for inactive neurons
            if len(current) > 1:

                neuron_variance = np.var(
                    h_cur,
                    axis=0
                )

                inactive_fraction = np.mean(
                    neuron_variance < 1e-8
                )

                if inactive_fraction > 0.30:

                    log.warning(
                        "PADD: %.1f%% inactive neurons",
                        inactive_fraction * 100
                    )

            if self._past_activations is None:

                if (
                    reference is not None
                    and len(reference) > 0
                ):

                    h_ref = self._get_activations(
                        reference
                    )

                    self._reset_history(
                        h_ref
                    )

                else:

                    self._reset_history(
                        h_cur
                    )

                return None

            result = self._run_test(
                self._past_activations,
                h_cur
            )

            if result is not None:

                log.info(
                    "PADD drift detected "
                    "(score=%.3f)",
                    result["score"]
                )

                self._reset_history(
                    h_cur
                )

                return result

            self._append_history(
                h_cur
            )

        except Exception as e:

            log.warning(
                "PADD error: %s",
                str(e)
            )

        return None