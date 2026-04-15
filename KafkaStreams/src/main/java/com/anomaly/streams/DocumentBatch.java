package com.anomaly.streams;

import java.util.List;

public class DocumentBatch {

    private String batchId;
    private String windowType;
    private long windowStart;
    private long windowEnd;
    private int documentCount;
    private List<ProcessedDocument> documents;

    public DocumentBatch() {}

    public DocumentBatch(String batchId, String windowType,
                         long windowStart, long windowEnd,
                         List<ProcessedDocument> documents) {
        this.batchId = batchId;
        this.windowType = windowType;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.documents = documents;
        this.documentCount = documents.size();
    }

    public String getBatchId() { return batchId; }
    public void setBatchId(String batchId) { this.batchId = batchId; }

    public String getWindowType() { return windowType; }
    public void setWindowType(String windowType) { this.windowType = windowType; }

    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    public int getDocumentCount() { return documentCount; }
    public void setDocumentCount(int documentCount) { this.documentCount = documentCount; }

    public List<ProcessedDocument> getDocuments() { return documents; }
    public void setDocuments(List<ProcessedDocument> documents) {
        this.documents = documents;
        this.documentCount = documents != null ? documents.size() : 0;
    }

    public static class ProcessedDocument {

        private String docId;
        private long timestamp;
        private String sourceCategory;
        private long sequenceNumber;
        private String cleanedText;
        private List<String> tokens;

        public ProcessedDocument() {}

        public String getDocId() { return docId; }
        public void setDocId(String docId) { this.docId = docId; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        public String getSourceCategory() { return sourceCategory; }
        public void setSourceCategory(String sourceCategory) { this.sourceCategory = sourceCategory; }

        public long getSequenceNumber() { return sequenceNumber; }
        public void setSequenceNumber(long sequenceNumber) { this.sequenceNumber = sequenceNumber; }

        public String getCleanedText() { return cleanedText; }
        public void setCleanedText(String cleanedText) { this.cleanedText = cleanedText; }

        public List<String> getTokens() { return tokens; }
        public void setTokens(List<String> tokens) { this.tokens = tokens; }
    }
}