package com.anomaly.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BatchAggregator {

    private static final Logger log = LoggerFactory.getLogger(BatchAggregator.class);

    private final TextPreprocessor preprocessor = new TextPreprocessor();
    private final ObjectMapper mapper = new ObjectMapper();

    public List<DocumentBatch.ProcessedDocument> initialize() {
        return new ArrayList<>();
    }

    public List<DocumentBatch.ProcessedDocument> aggregate(
            String key, String value,
            List<DocumentBatch.ProcessedDocument> aggregate) {
        try {
            JsonNode node = mapper.readTree(value);

            String rawText = node.has("text") ? node.get("text").asText() : "";
            String cleaned = preprocessor.clean(rawText);

            if (cleaned.isBlank())
                return aggregate;

            DocumentBatch.ProcessedDocument doc = new DocumentBatch.ProcessedDocument();
            doc.setDocId(node.has("docId") ? node.get("docId").asText() : UUID.randomUUID().toString());
            doc.setTimestamp(node.has("timestamp") ? node.get("timestamp").asLong() : System.currentTimeMillis());
            doc.setSourceCategory(node.has("sourceCategory") ? node.get("sourceCategory").asText() : key);
            doc.setSequenceNumber(node.has("sequenceNumber") ? node.get("sequenceNumber").asLong() : -1);
            doc.setCleanedText(cleaned);
            doc.setDriftLabel(node.has("driftLabel") && node.get("driftLabel").asBoolean());
            doc.setDriftType(node.has("driftType") && !node.get("driftType").isNull() ? node.get("driftType").asText() : null);
            doc.setDriftStartTs(node.has("driftStartTs") && !node.get("driftStartTs").isNull() ? node.get("driftStartTs").asLong() : null);

            aggregate.add(doc);
        } catch (Exception e) {
            log.warn("Failed to parse/preprocess record: {}", e.getMessage());
        }
        return aggregate;
    }
}