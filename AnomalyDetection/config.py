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

ACTIVE_DETECTORS = ["mmd", "isolation_forest", "padd"]
# ACTIVE_DETECTORS = ["mmd", "padd"] # Swap if you want to drop IF

MMD_THRESHOLD   = 0.05
MMD_SAMPLE_SIZE = 200

IF_CONTAMINATION = 0.05
IF_N_ESTIMATORS  = 100
IF_THRESHOLD     = -0.50

PADD_HIDDEN_DIM   = 100
PADD_ALPHA        = 0.01  # Significance level for t-test
PADD_THRESHOLD    = 0.1   # Fraction of tests rejecting null
PADD_REPLICATIONS = 20    # Number of replications
PADD_SAMPLE_SIZE  = 50    # Size of each sample in replication

NOISE_WARMUP = False
NOISE_WARMUP_SEED = 42
