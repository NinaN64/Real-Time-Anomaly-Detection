package com.anomaly.streams;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsApp {

    private static final Logger log = LoggerFactory.getLogger(StreamsApp.class);

    public static void main(String[] args) throws Exception {

        Properties appProps = new Properties();
        try (InputStream in = StreamsApp.class
                .getClassLoader()
                .getResourceAsStream("streams.properties")) {
            if (in == null) throw new RuntimeException("streams.properties not found on classpath");
            appProps.load(in);
        }

        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG,
                         appProps.getProperty("application.id", "anomaly-streams-v1"));
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                         appProps.getProperty("bootstrap.servers", "localhost:9092"));
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                         appProps.getProperty("num.stream.threads", "6"));
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG,
                         appProps.getProperty("state.dir", "/tmp/kafka-streams/anomaly"));

        ensureTopicsExist(appProps);

        Topology topology = new StreamTopology(appProps).build();
        KafkaStreams streams = new KafkaStreams(topology, streamsProps);

        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Uncaught exception in stream thread {}: {}",
                      thread.getName(), throwable.getMessage(), throwable);
        });

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — closing streams.");
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            log.info("Kafka Streams started. Waiting for records...");
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("Streams application stopped.");
    }

    private static void ensureTopicsExist(Properties appProps) {
        String bootstrap  = appProps.getProperty("bootstrap.servers", "localhost:9092");
        String slidingOut = appProps.getProperty("topic.output.sliding", "preprocessed-batches-sliding");

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            NewTopic sliding = new NewTopic(slidingOut, 6, (short) 1);
            admin.createTopics(Collections.singletonList(sliding));

            log.info("Output topic ensured: {}", slidingOut);
        } catch (Exception e) {
            log.debug("Topic creation skipped (may already exist): {}", e.getMessage());
        }
    }
}