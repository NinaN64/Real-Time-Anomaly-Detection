import json
import logging
from typing import Callable

from confluent_kafka import Consumer, KafkaException
import config

log = logging.getLogger("consumer")


def build_consumer() -> Consumer:
    topic = config.INPUT_TOPIC_SLIDING

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([topic])
    log.info("Subscribed to topic: %s", topic)
    return consumer


def run(batch_handler: Callable[[dict], None]) -> None:
    consumer = build_consumer()


    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                batch = json.loads(msg.value().decode("utf-8"))
                doc_count = batch.get("documentCount", 0)
                log.debug(
                    "batch | docs=%d | start=%d",
                    doc_count, batch.get("windowStart", 0)
                )
                if doc_count > 0:
                    batch_handler(batch)
            except json.JSONDecodeError as e:
                log.warning("Failed to deserialize batch: %s", e)

    except KeyboardInterrupt:
        log.info("Consumer stopped by user.")
    finally:
        consumer.close()
        log.info("Consumer closed.")