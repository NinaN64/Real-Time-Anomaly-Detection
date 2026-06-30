package com.anomaly.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class ProducerApp {

    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);

    public static void main(String[] args) throws Exception {

        String dataset = "newsgroups";
        long seed = -1; // -1 means no subsampling
        int subsetSize = 2000; // docs per bucket when subsampling is active
        long maxSeq = 4500;

        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--dataset"))
                dataset = args[i + 1];
            if (args[i].equals("--seed"))
                seed = Long.parseLong(args[i + 1]);
            if (args[i].equals("--subset-size"))
                subsetSize = Integer.parseInt(args[i + 1]);
        }

        Properties props = new Properties();
        try (InputStream in = ProducerApp.class
                .getClassLoader()
                .getResourceAsStream("producer.properties")) {
            if (in == null) {
                throw new RuntimeException("producer.properties not found on classpath");
            }
            props.load(in);
        }

        long rateMs = Long.parseLong(props.getProperty("producer.rate.ms", "100"));
        int gradualWin = Integer.parseInt(props.getProperty("drift.gradual.window", "200"));
        int recurPeriod = Integer.parseInt(props.getProperty("drift.recurring.period", "150"));

        String datasetPath, initialCategory, suddenTarget, gradualTarget, recurringTarget;

        switch (dataset) {
            case "agnews":
                datasetPath = "Data/agnews.jsonl";
                initialCategory = "agnews_world";
                suddenTarget = "agnews_sports";
                gradualTarget = "agnews_scitech";
                recurringTarget = "agnews_world";
                break;
            case "yahoo":
                datasetPath = "Data/yahoo.jsonl";
                initialCategory = "yahoo_science";
                suddenTarget = "yahoo_relationships";
                gradualTarget = "yahoo_politics";
                recurringTarget = "yahoo_science";
                break;
            default:
                datasetPath = "Data/20newsgroups.jsonl";
                initialCategory = "sci.space";
                suddenTarget = "talk.politics.guns";
                gradualTarget = "sci.med";
                recurringTarget = "sci.space";
                break;
        }

        log.info("Dataset: {}  Path: {}", dataset, datasetPath);

        DatasetLoader loader = new DatasetLoader(datasetPath);

        if (seed >= 0) {
            log.info("Subsampling enabled: seed={}, subsetSize={}", seed, subsetSize);
            loader.subsample(subsetSize, seed);
        } else {
            log.info("No subsampling — using full dataset");
        }

        DriftInjector injector = new DriftInjector(loader, initialCategory, gradualWin, recurPeriod);

        try (DocumentProducer producer = new DocumentProducer(props, loader, injector)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal — stopping producer.");
                producer.stop();
            }));

            final String sudden = suddenTarget;
            final String gradual = gradualTarget;
            final String recurring = recurringTarget;

            Thread driftScheduler = new Thread(() -> {
                try {
                    Thread.sleep(rateMs * 1000);
                    injector.triggerSudden(sudden, 1000);

                    Thread.sleep(rateMs * 1000);
                    injector.triggerGradual(gradual, 2000);

                    Thread.sleep(rateMs * (gradualWin + 200L));
                    injector.triggerRecurring(recurring, 3200);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            driftScheduler.setDaemon(true);
            driftScheduler.start();

            producer.stream(rateMs, maxSeq);
        }
    }
}