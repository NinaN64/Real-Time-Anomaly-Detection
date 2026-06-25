package com.anomaly.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class DocumentProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DocumentProducer.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final DatasetLoader loader;
    private final DriftInjector driftInjector;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicLong sequenceCounter = new AtomicLong(0);

    private volatile boolean running = true;

    public DocumentProducer(Properties appProps, DatasetLoader loader, DriftInjector driftInjector) {
        this.topic = appProps.getProperty("topic.raw.documents", "raw-documents");
        this.loader = loader;
        this.driftInjector = driftInjector;

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                appProps.getProperty("bootstrap.servers", "localhost:9092"));
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, appProps.getProperty("acks", "all"));
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, appProps.getProperty("retries", "3"));
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, appProps.getProperty("linger.ms", "5"));
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, appProps.getProperty("batch.size", "16384"));
        kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, appProps.getProperty("compression.type", "snappy"));

        this.producer = new KafkaProducer<>(kafkaProps);
        log.info("DocumentProducer ready. Topic: {}", topic);
    }

    public void stream(long rateMs, long maxSeq) {
        if (maxSeq > 0) {
            log.info("Streaming started. Rate: {}ms per message. Max seq: {}", rateMs, maxSeq);
        } else {
            log.info("Streaming started. Rate: {}ms per message. No seq limit.", rateMs);
        }

        while (running) {
            try {
                long seq = sequenceCounter.getAndIncrement();

                if (maxSeq > 0 && seq >= maxSeq) {
                    log.info("Reached max seq {} — stopping producer.", maxSeq);
                    break;
                }

                String category = driftInjector.resolveCategory(seq);
                String text = loader.nextDocument(category);

                NewsDocument doc = new NewsDocument(
                        UUID.randomUUID().toString(),
                        System.currentTimeMillis(),
                        text,
                        category,
                        seq);

                driftInjector.annotate(doc, seq);

                String json = mapper.writeValueAsString(doc);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, category, json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Send failed seq={}: {}", seq, exception.getMessage());
                    } else if (seq % 500 == 0) {
                        log.info("seq={} | category={} | drift={} | partition={} | offset={}",
                                seq, doc.getSourceCategory(), doc.isDriftLabel(),
                                metadata.partition(), metadata.offset());
                    }
                });

                if (rateMs > 0)
                    Thread.sleep(rateMs);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Stream interrupted.");
                break;
            } catch (Exception e) {
                log.error("Streaming error: {}", e.getMessage(), e);
            }
        }

        log.info("Stream stopped. Total sent: {}", sequenceCounter.get());
    }

    public void stop() {
        log.info("Stop requested.");
        running = false;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
        log.info("Kafka producer closed.");
    }
}