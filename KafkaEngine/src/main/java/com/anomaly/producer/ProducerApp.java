package com.anomaly.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class ProducerApp {

    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);

    public static void main(String[] args) throws Exception {

        String dataset = "newsgroups";
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--dataset")) {
                dataset = args[i + 1];
            }
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

        String datasetPath;
        String initialCategory;
        String suddenTarget;
        String gradualTarget;
        String recurringTarget;

        switch (dataset) {
            case "wikipedia":
                datasetPath = "Data/wikipedia.jsonl";
                initialCategory = "wikipedia_science";
                suddenTarget = "wikipedia_politics";
                gradualTarget = "wikipedia_sports";
                recurringTarget = "wikipedia_politics";
                break;
            case "arxiv":
                datasetPath = "Data/arxiv.jsonl";
                initialCategory = "arxiv_math";
                suddenTarget = "arxiv_cs";
                gradualTarget = "arxiv_physics";
                recurringTarget = "arxiv_cs";
                break;
            default:
                datasetPath = "Data/20newsgroups.jsonl";
                initialCategory = "sci.space";
                suddenTarget = "talk.politics.guns";
                gradualTarget = "sci.med";
                recurringTarget = "comp.graphics";
                break;
        }

        log.info("Dataset: {}  Path: {}", dataset, datasetPath);

        DatasetLoader loader = new DatasetLoader(datasetPath);
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

            producer.stream(rateMs);
        }
    }
}