package com.anomaly.producer;

import com.fasterxml.jackson.annotation.JsonInclude;

// Data transfer object for Kafka news streams
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NewsDocument {

    private String docId;
    private long timestamp;
    private String text;
    private String sourceCategory;
    private long sequenceNumber;

    private boolean driftLabel;
    private String driftType;
    private Long driftStartTs;

    public NewsDocument() {
    }

    public NewsDocument(String docId, long timestamp, String text,
            String sourceCategory, long sequenceNumber) {
        this.docId = docId;
        this.timestamp = timestamp;
        this.text = text;
        this.sourceCategory = sourceCategory;
        this.sequenceNumber = sequenceNumber;
        this.driftLabel = false;
        this.driftType = null;
        this.driftStartTs = null;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getSourceCategory() {
        return sourceCategory;
    }

    public void setSourceCategory(String sourceCategory) {
        this.sourceCategory = sourceCategory;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public boolean isDriftLabel() {
        return driftLabel;
    }

    public void setDriftLabel(boolean driftLabel) {
        this.driftLabel = driftLabel;
    }

    public String getDriftType() {
        return driftType;
    }

    public void setDriftType(String driftType) {
        this.driftType = driftType;
    }

    public Long getDriftStartTs() {
        return driftStartTs;
    }

    public void setDriftStartTs(Long driftStartTs) {
        this.driftStartTs = driftStartTs;
    }

    @Override
    public String toString() {
        return "NewsDocument{docId='" + docId + "', category='" + sourceCategory +
                "', drift=" + driftLabel + ", seq=" + sequenceNumber + "}";
    }
}