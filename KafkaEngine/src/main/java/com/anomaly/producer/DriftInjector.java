package com.anomaly.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

// implementation of concept drift injector
public class DriftInjector {

    private static final Logger log = LoggerFactory.getLogger(DriftInjector.class);

    public enum DriftType { SUDDEN, GRADUAL, RECURRING }

    private final DatasetLoader loader;
    private final Random random = new Random();

    private String currentCategory;
    private String targetCategory;
    private DriftType activeDriftType;
    private long driftStartSeq;
    private boolean inDrift = false;

    private final int gradualWindow;
    private long gradualProgress = 0;

    private final int recurringPeriod;
    private long recurringCounter = 0;

    public DriftInjector(DatasetLoader loader, String initialCategory,
                         int gradualWindow, int recurringPeriod) {
        this.loader = loader;
        this.currentCategory = initialCategory;
        this.gradualWindow = gradualWindow;
        this.recurringPeriod = recurringPeriod;
        log.info("DriftInjector initialized. Starting category: {}", initialCategory);
    }

    public synchronized void triggerSudden(String targetCategory, long atSequence) {
        log.info("SUDDEN drift at seq={}: {} -> {}", atSequence, currentCategory, targetCategory);
        this.targetCategory = targetCategory;
        this.activeDriftType = DriftType.SUDDEN;
        this.driftStartSeq = atSequence;
        this.inDrift = true;
        this.currentCategory = targetCategory;
    }

    public synchronized void triggerGradual(String targetCategory, long atSequence) {
        log.info("GRADUAL drift at seq={}: {} -> {} over {} docs",
                 atSequence, currentCategory, targetCategory, gradualWindow);
        this.targetCategory = targetCategory;
        this.activeDriftType = DriftType.GRADUAL;
        this.driftStartSeq = atSequence;
        this.gradualProgress = 0;
        this.inDrift = true;
    }

    public synchronized void triggerRecurring(String targetCategory, long atSequence) {
        log.info("RECURRING drift at seq={}: {} <-> {} every {} docs",
                 atSequence, currentCategory, targetCategory, recurringPeriod);
        this.targetCategory = targetCategory;
        this.activeDriftType = DriftType.RECURRING;
        this.driftStartSeq = atSequence;
        this.recurringCounter = 0;
        this.inDrift = true;
    }

    public synchronized String resolveCategory(long sequenceNumber) {
        if (!inDrift) return currentCategory;

        switch (activeDriftType) {
            case SUDDEN:    return currentCategory;
            case GRADUAL:   return resolveGradual(sequenceNumber);
            case RECURRING: return resolveRecurring();
            default:        return currentCategory;
        }
    }

    public synchronized void annotate(NewsDocument doc, long sequenceNumber) {
        if (!inDrift) {
            doc.setDriftLabel(false);
            doc.setDriftType(null);
            doc.setDriftStartTs(null);
            return;
        }

        doc.setDriftLabel(true);
        doc.setDriftStartTs(driftStartSeq);
        switch (activeDriftType) {
            case SUDDEN:    doc.setDriftType("sudden");    break;
            case GRADUAL:   doc.setDriftType("gradual");   break;
            case RECURRING: doc.setDriftType("recurring"); break;
            default:        doc.setDriftType(null);
        }
    }

    private String resolveGradual(long sequenceNumber) {
        double mixRatio = Math.min(1.0, (double) gradualProgress / gradualWindow);
        gradualProgress++;

        if (gradualProgress >= gradualWindow) {
            log.info("GRADUAL drift complete at seq={}: now fully on '{}'",
                     sequenceNumber, targetCategory);
            currentCategory = targetCategory;
            inDrift = false;
            return currentCategory;
        }

        return (random.nextDouble() < mixRatio) ? targetCategory : currentCategory;
    }

    private String resolveRecurring() {
        boolean onTarget = (recurringCounter / recurringPeriod) % 2 == 0;
        recurringCounter++;
        return onTarget ? targetCategory : currentCategory;
    }

    public String getCurrentCategory() { return currentCategory; }
    public boolean isInDrift() { return inDrift; }
    public DriftType getActiveDriftType() { return activeDriftType; }
}