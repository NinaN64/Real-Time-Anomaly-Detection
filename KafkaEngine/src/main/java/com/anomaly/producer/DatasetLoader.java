package com.anomaly.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.Collections;

public class DatasetLoader {

    private static final Logger log = LoggerFactory.getLogger(DatasetLoader.class);

    private final Map<String, List<String>> documentsByCategory = new HashMap<>();
    private final Map<String, Integer> cursors = new HashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();
    private Random random = new Random(42);

    public DatasetLoader(String jsonlPath) throws IOException {
        load(jsonlPath);
    }

    private void load(String path) throws IOException {
        log.info("Loading dataset from {}", path);
        int total = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty())
                    continue;

                JsonNode node = mapper.readTree(line);
                String text = node.get("text").asText();
                String category = node.get("category").asText();

                documentsByCategory
                        .computeIfAbsent(category, k -> new ArrayList<>())
                        .add(text);
                total++;
            }
        }

        log.info("Loaded {} documents across {} categories", total, documentsByCategory.size());
        documentsByCategory.forEach((cat, docs) -> log.info("  {}: {} docs", cat, docs.size()));
    }

    public void subsample(int docsPerBucket, long seed) {
        random = new Random(seed);
        log.info("Subsampling: {} docs per bucket, seed={}", docsPerBucket, seed);

        for (Map.Entry<String, List<String>> entry : documentsByCategory.entrySet()) {
            List<String> docs = entry.getValue();
            Collections.shuffle(docs, random);
            if (docs.size() > docsPerBucket) {
                List<String> sampled = new ArrayList<>(docs.subList(0, docsPerBucket));
                entry.setValue(sampled);
                log.info("  {}: {} -> {} docs", entry.getKey(), docs.size(), docsPerBucket);
            } else {
                log.info("  {}: {} docs (no trim needed)", entry.getKey(), docs.size());
            }
        }
        cursors.clear();
    }

    public String nextDocument(String category) {
        List<String> docs = documentsByCategory.get(category);
        if (docs == null || docs.isEmpty()) {
            throw new IllegalArgumentException("Unknown or empty category: " + category);
        }

        int idx = cursors.getOrDefault(category, 0);
        if (idx >= docs.size()) {
            Collections.shuffle(docs, random);
            idx = 0;
            log.debug("Reshuffled category '{}' — starting second pass", category);
        }

        String doc = docs.get(idx);
        cursors.put(category, idx + 1);
        return doc;
    }

    public Set<String> getCategories() {
        return Collections.unmodifiableSet(documentsByCategory.keySet());
    }

    public List<String> getCategoriesInGroup(String group) {
        List<String> result = new ArrayList<>();
        for (String cat : documentsByCategory.keySet()) {
            if (cat.startsWith(group + ".") || cat.equals(group)) {
                result.add(cat);
            }
        }
        return result;
    }

    public int getCategorySize(String category) {
        return documentsByCategory.getOrDefault(category, Collections.emptyList()).size();
    }
}