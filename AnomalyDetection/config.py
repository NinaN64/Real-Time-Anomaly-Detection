BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC_TUMBLING = "preprocessed-batches-tumbling"
INPUT_TOPIC_SLIDING  = "preprocessed-batches-sliding"
ALERT_TOPIC          = "drift-alerts"
CONSUMER_GROUP       = "anomaly-detection-group"

WINDOW_MODE = "tumbling"

SBERT_MODEL = "all-MiniLM-L6-v2"

EMBEDDING_STORE_SIZE = 200
MIN_EMBEDDINGS_BEFORE_DETECTION = 100

TRIGGER_N = 100

ACTIVE_DETECTORS = ["mmd", "isolation_forest"]

MMD_THRESHOLD   = 0.05
MMD_SAMPLE_SIZE = 200

IF_CONTAMINATION = 0.05
IF_N_ESTIMATORS  = 100
IF_THRESHOLD     = -0.50
