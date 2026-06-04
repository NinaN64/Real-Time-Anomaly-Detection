import numpy as np
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from detectors.padd_detector import PADDDetector

logging.basicConfig(level=logging.INFO)

def test_padd():
    detector = PADDDetector(hidden_dim=50, replications=10, sample_size=30)
    
    input_dim = 384
    reference = np.random.normal(0, 1, (200, input_dim))
    
    current_no_drift = np.random.normal(0, 1, (50, input_dim))
    alert_none = detector.update(current_no_drift, reference)
    print(f"No drift test: {'ALERT' if alert_none else 'Pass (no alert)'}")
    
    current_drift = np.random.normal(0.5, 1, (50, input_dim))
    alert_drift = detector.update(current_drift, reference)
    print(f"Drift test (mean shift): {'ALERT' if alert_drift else 'Fail (no alert)'}")
    if alert_drift:
        print(f"  Score: {alert_drift['score']}, Threshold: {alert_drift['threshold']}")

    current_vars = np.random.normal(0, 2, (50, input_dim))
    alert_vars = detector.update(current_vars, reference)
    print(f"Drift test (var shift): {'ALERT' if alert_vars else 'Fail (no alert)'}")

if __name__ == "__main__":
    test_padd()
