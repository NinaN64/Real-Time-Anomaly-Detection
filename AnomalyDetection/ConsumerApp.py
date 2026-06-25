import os
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"

from vectorizer import Vectorizer
import argparse
import logging
import random
import numpy as np
import config
from consumer import run as run_consumer
from embedding_store import EmbeddingStore
from alert_publisher import AlertPublisher
from detectors import load_detectors
from nltk.corpus import words as nltk_words

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("anomaly")


def generate_noise_documents(n: int, seed: int, words_per_doc: int = 20) -> list[str]:
    """Generate n synthetic noise documents from random English words."""
    rng = random.Random(seed)
    vocabulary = nltk_words.words()
    return [
        " ".join(rng.choices(vocabulary, k=words_per_doc))
        for _ in range(n)
    ]


def main():
    parser = argparse.ArgumentParser(description="Anomaly Detection Consumer")
    parser.add_argument(
        "--trigger-n",
        type=int,
        default=config.TRIGGER_N,
        help=f"Number of embeddings to accumulate before triggering detectors (default: {config.TRIGGER_N})"
    )
    parser.add_argument(
        "--noise-warmup",
        action="store_true",
        default=config.NOISE_WARMUP,
        help="Pre-fill reference window with synthetic noise documents instead of real stream documents"
    )
    args = parser.parse_args()
    trigger_n = args.trigger_n
    noise_warmup = args.noise_warmup

    log.info(
        "starting up  model=%s  detectors=%s  trigger=%d  noise_warmup=%s",
        config.SBERT_MODEL, config.ACTIVE_DETECTORS, trigger_n, noise_warmup
    )

    vectorizer = Vectorizer()
    store      = EmbeddingStore()
    publisher  = AlertPublisher()
    detectors  = load_detectors(config.ACTIVE_DETECTORS)

    # pre-fill reference window with synthetic noise documents embedded through SBERT
    if noise_warmup:
        log.info("generating %d synthetic noise documents for warm-up...",
                 config.MIN_EMBEDDINGS_BEFORE_DETECTION)
        noise_docs = generate_noise_documents(
            n=config.MIN_EMBEDDINGS_BEFORE_DETECTION,
            seed=config.NOISE_WARMUP_SEED
        )
        noise_embeddings = vectorizer.model.encode(
            noise_docs,
            show_progress_bar=False,
            convert_to_numpy=True
        ).astype(np.float32)
        for vec in noise_embeddings:
            store.add(np.expand_dims(vec, 0))
        log.info(
            "noise warm-up complete — %d synthetic embeddings loaded as reference",
            config.MIN_EMBEDDINGS_BEFORE_DETECTION
        )

    trigger_buffer  = []
    reference_frozen: np.ndarray = np.empty((0,))
    category_counts: dict = {}

    def handle_batch(batch: dict) -> None:
        nonlocal trigger_buffer, reference_frozen, category_counts

        # implementation of drift evaluation
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

        # ignore duplicate records
        last_doc_count = category_counts.get(category, 0)

        if docs_total <= last_doc_count:
            return

        # generate embeddings
        new_embeddings = vectorizer.embed_batch(batch, start_idx=last_doc_count)
        category_counts[category] = docs_total

        if new_embeddings.shape[0] == 0:
            return

        # populate store or trigger buffer
        for vec in new_embeddings:
            if not store.is_ready():
                store.add(np.expand_dims(vec, 0))
            else:
                trigger_buffer.append(vec)

        # wait for warm-up
        if not store.is_ready():
            if store.total_seen % 25 == 0:
                log.info("warming up  %d / %d",
                         store.total_seen, config.MIN_EMBEDDINGS_BEFORE_DETECTION)
            return

        # freeze reference window
        if reference_frozen.shape[0] == 0:
            reference_frozen = store.get_reference()
            log.info("\n  reference window ready — %d embeddings, detection starting\n",
                     len(reference_frozen))

        # slide window evaluation
        while len(trigger_buffer) >= trigger_n:
            current_docs = trigger_buffer[:trigger_n]
            trigger_buffer = trigger_buffer[trigger_n:]
            evaluate_trigger(current_docs)

    try:
        run_consumer(handle_batch)
    finally:
        publisher.flush()
        log.info("Pipeline shut down.")


if __name__ == "__main__":
    main()