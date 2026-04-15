package com.anomaly.streams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;

public class DocumentListSerde implements Serde<List<DocumentBatch.ProcessedDocument>> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<DocumentBatch.ProcessedDocument>> TYPE_REF =
        new TypeReference<List<DocumentBatch.ProcessedDocument>>() {};

    @Override
    public Serializer<List<DocumentBatch.ProcessedDocument>> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize document list", e);
            }
        };
    }

    @Override
    public Deserializer<List<DocumentBatch.ProcessedDocument>> deserializer() {
        return (topic, data) -> {
            if (data == null) return new ArrayList<>();
            try {
                return MAPPER.readValue(data, TYPE_REF);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize document list", e);
            }
        };
    }
}