from detectors.mmd_detector import MMDDetector
from detectors.isolation_forest_detector import IsolationForestDetector

REGISTRY = {
    "mmd":               MMDDetector,
    "isolation_forest":  IsolationForestDetector,
}


def load_detectors(names: list) -> list:
    detectors = []
    for name in names:
        if name not in REGISTRY:
            raise ValueError(f"Unknown detector: '{name}'. Available: {list(REGISTRY.keys())}")
        detectors.append(REGISTRY[name]())
    return detectors