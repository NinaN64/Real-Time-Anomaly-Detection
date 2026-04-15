import logging
import numpy as np
import config
from consumer import run as run_consumer
from vectorizer import Vectorizer
from embedding_store import EmbeddingStore
from alert_publisher import AlertPublisher
from detectors import load_detectors

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("ConsumerApp")


def main():
    log.info("Starting AnomalyDetection pipeline")
    log.info("SBERT model:      %s", config.SBERT_MODEL)
    log.info("Store size:       %d", config.EMBEDDING_STORE_SIZE)
    log.info("Trigger every:    %d documents", config.TRIGGER_N)
    log.info("Active detectors: %s", config.ACTIVE_DETECTORS)

    vectorizer = Vectorizer()
    store      = EmbeddingStore()
    publisher  = AlertPublisher()
    detectors  = load_detectors(config.ACTIVE_DETECTORS)

    trigger_buffer  = []
    reference_frozen: np.ndarray = np.empty((0,))  # locked baseline after warm-up
    category_counts: dict = {}

    def handle_batch(batch: dict) -> None:
        nonlocal trigger_buffer, reference_frozen, category_counts

        def evaluate_trigger(docs: list) -> None:
            if len(docs) < 10:
                return
            current = np.stack(docs)
            log.info("Trigger fired on %d documents", len(current))
            for detector in detectors:
                alert = detector.update(current, reference_frozen)
                if alert:
                    log.warning(
                        "DRIFT ALERT | detector=%s | score=%.4f | window=[%d, %d]",
                        alert["detector"], alert["score"],
                        batch.get("windowStart", 0), batch.get("windowEnd", 0)
                    )
                    publisher.publish(alert, batch)

        documents = batch.get("documents", [])
        docs_total = len(documents)
        category = documents[0].get("sourceCategory", "unknown") if documents else "unknown"
        
        last_doc_count = category_counts.get(category, 0)
        
        if docs_total <= last_doc_count:
            return

        new_embeddings = vectorizer.embed_batch(batch, start_idx=last_doc_count)
        category_counts[category] = docs_total

        if new_embeddings.shape[0] == 0:
            return

        for vec in new_embeddings:
            if not store.is_ready():
                store.add(np.expand_dims(vec, 0))
            else:
                trigger_buffer.append(vec)

        if not store.is_ready():
            if new_embeddings.shape[0] > 0:
                log.info("Warming up... (%d/%d embeddings)",
                         store.total_seen, config.MIN_EMBEDDINGS_BEFORE_DETECTION)
            return

        if reference_frozen.shape[0] == 0:
            reference_frozen = store.get_reference()
            log.info("Reference window frozen: %d embeddings", len(reference_frozen))

        while len(trigger_buffer) >= config.TRIGGER_N:
            current_docs = trigger_buffer[:config.TRIGGER_N]
            trigger_buffer = trigger_buffer[config.TRIGGER_N:]
            evaluate_trigger(current_docs)

    try:
        run_consumer(handle_batch)
    finally:
        publisher.flush()
        log.info("Pipeline shut down.")


if __name__ == "__main__":
    main()