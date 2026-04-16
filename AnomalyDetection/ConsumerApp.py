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
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("anomaly")


def main():
    log.info(
        "starting up  model=%s  detectors=%s  trigger=%d",
        config.SBERT_MODEL, config.ACTIVE_DETECTORS, config.TRIGGER_N
    )

    vectorizer = Vectorizer()
    store      = EmbeddingStore()
    publisher  = AlertPublisher()
    detectors  = load_detectors(config.ACTIVE_DETECTORS)

    trigger_buffer  = []
    reference_frozen: np.ndarray = np.empty((0,))
    category_counts: dict = {}

    def handle_batch(batch: dict) -> None:
        nonlocal trigger_buffer, reference_frozen, category_counts

        def evaluate_trigger(docs: list) -> None:
            if len(docs) < 10:
                return
            current = np.stack(docs)
            log.info("── trigger: %d docs", len(current))
            for detector in detectors:
                alert = detector.update(current, reference_frozen)
                if alert:
                    log.warning(
                        "\n  ╔══════════════════════════════════════╗"
                        "\n  ║  DRIFT DETECTED                      ║"
                        "\n  ║  detector : %-26s║"
                        "\n  ║  score    : %-26.4f║"
                        "\n  ║  threshold: %-26.4f║"
                        "\n  ╚══════════════════════════════════════╝",
                        alert["detector"], alert["score"], alert["threshold"]
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
            if store.total_seen % 25 == 0:
                log.info("warming up  %d / %d",
                         store.total_seen, config.MIN_EMBEDDINGS_BEFORE_DETECTION)
            return

        if reference_frozen.shape[0] == 0:
            reference_frozen = store.get_reference()
            log.info("\n  reference window ready — %d embeddings, detection starting\n", len(reference_frozen))

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