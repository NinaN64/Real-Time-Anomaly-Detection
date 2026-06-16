import json
import logging

from confluent_kafka import Producer
import config

log = logging.getLogger("alert_publisher")


# implementation of alert publishing
class AlertPublisher:

    def __init__(self):
        self._producer = Producer({
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        })
        log.info("AlertPublisher ready. Topic: %s", config.ALERT_TOPIC)

    def publish(self, alert: dict, batch: dict) -> None:
        message = {
            **alert,
            "windowType":  batch.get("windowType"),
            "windowStart": batch.get("windowStart"),
            "windowEnd":   batch.get("windowEnd"),
            "docCount":    batch.get("documentCount"),
        }

        try:
            self._producer.produce(
                topic=config.ALERT_TOPIC,
                key=alert["detector"],
                value=json.dumps(message).encode("utf-8"),
                callback=self._delivery_report,
            )
            self._producer.poll(0)
        except Exception as e:
            log.error("Failed to publish alert: %s", e)

    def flush(self) -> None:
        self._producer.flush()

    def _delivery_report(self, err, msg) -> None:
        if err:
            log.error("Alert delivery failed: %s", err)
        else:
            log.debug("Alert delivered to %s [partition %d]",
                      msg.topic(), msg.partition())