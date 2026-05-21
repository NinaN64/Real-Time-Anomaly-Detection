import numpy as np
import logging
import sys
import os

# Add the AnomalyDetection directory to path
sys.path.append(os.path.abspath("c:/Users/ninoc/Documents/Studia/Magisterka/new/Real-Time-Anomaly-Detection/AnomalyDetection"))

from detectors.padd_detector import PADDDetector

logging.basicConfig(level=logging.INFO)

def test_padd():
    detector = PADDDetector(hidden_dim=50, replications=10, sample_size=30)
    
    # Create reference data (normal distribution)
    input_dim = 384 # SBERT embedding dim
    reference = np.random.normal(0, 1, (200, input_dim))
    
    # 1. Test with similar distribution (no drift)
    current_no_drift = np.random.normal(0, 1, (50, input_dim))
    alert_none = detector.update(current_no_drift, reference)
    print(f"No drift test: {'ALERT' if alert_none else 'Pass (no alert)'}")
    
    # 2. Test with shifted distribution (mean shift)
    current_drift = np.random.normal(0.5, 1, (50, input_dim))
    alert_drift = detector.update(current_drift, reference)
    print(f"Drift test (mean shift): {'ALERT' if alert_drift else 'Fail (no alert)'}")
    if alert_drift:
        print(f"  Score: {alert_drift['score']}, Threshold: {alert_drift['threshold']}")

    # 3. Test with variance shift
    current_vars = np.random.normal(0, 2, (50, input_dim))
    alert_vars = detector.update(current_vars, reference)
    print(f"Drift test (var shift): {'ALERT' if alert_vars else 'Fail (no alert)'}")

if __name__ == "__main__":
    test_padd()
