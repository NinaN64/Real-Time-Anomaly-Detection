package com.anomaly.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StreamTopology {

    private static final Logger log = LoggerFactory.getLogger(StreamTopology.class);

    private final Properties props;
    private final ObjectMapper mapper = new ObjectMapper();
    private final BatchAggregator aggregator = new BatchAggregator();
    private final Serde<String> stringSerde = Serdes.String();

    public StreamTopology(Properties props) {
        this.props = props;
    }

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        String inputTopic = props.getProperty("topic.input", "raw-documents");
        String slidingOut = props.getProperty("topic.output.sliding", "preprocessed-batches-sliding");
        int minBatch = Integer.parseInt(props.getProperty("window.min.batch.size", "5"));
        long slidingSize = Long.parseLong(props.getProperty("window.sliding.size.seconds", "60"));
        long slidingAdv = Long.parseLong(props.getProperty("window.sliding.advance.seconds", "15"));

        KStream<String, String> source = builder.stream(
                inputTopic, Consumed.with(stringSerde, stringSerde));

        buildSliding(source, slidingOut, slidingSize, slidingAdv, minBatch);

        Topology topology = builder.build();
        log.info("Topology built. Strategy: SLIDING");
        return topology;
    }

    private void buildSliding(KStream<String, String> source,
            String outputTopic,
            long sizeSeconds,
            long advanceSeconds,
            int minBatch) {

        Materialized<String, List<DocumentBatch.ProcessedDocument>, WindowStore<Bytes, byte[]>> store = Materialized.<String, List<DocumentBatch.ProcessedDocument>, WindowStore<Bytes, byte[]>>as(
                "sliding-store")
                .withKeySerde(stringSerde)
                .withValueSerde(new DocumentListSerde());

        source
                .groupByKey(Grouped.with(stringSerde, stringSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(sizeSeconds))
                        .advanceBy(Duration.ofSeconds(advanceSeconds)))
                .aggregate(
                        () -> aggregator.initialize(),
                        (key, value, agg) -> aggregator.aggregate(key, value, agg),
                        store)
                .toStream()
                .filter((wk, docs) -> docs != null && docs.size() >= minBatch)
                .map((wk, docs) -> new KeyValue<>(
                        wk.key(),
                        serializeBatch(docs, "sliding", wk.window().start(), wk.window().end())))
                .filter((k, v) -> v != null)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        log.info("Sliding topology: {}s advance {}s -> {}", sizeSeconds, advanceSeconds, outputTopic);
    }

    private String serializeBatch(List<DocumentBatch.ProcessedDocument> docs,
            String windowType, long start, long end) {
        try {
            DocumentBatch batch = new DocumentBatch(
                    UUID.randomUUID().toString(), windowType, start, end, docs);
            return mapper.writeValueAsString(batch);
        } catch (Exception e) {
            log.error("Failed to serialize batch: {}", e.getMessage());
            return null;
        }
    }
}