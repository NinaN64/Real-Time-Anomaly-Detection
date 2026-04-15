package com.anomaly.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class ProducerApp {

    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        try (InputStream in = ProducerApp.class
                .getClassLoader()
                .getResourceAsStream("producer.properties")) {
            if (in == null) {
                throw new RuntimeException("producer.properties not found on classpath");
            }
            props.load(in);
        }

        String datasetPath = props.getProperty("dataset.path", "Data/20newsgroups.jsonl");
        long   rateMs      = Long.parseLong(props.getProperty("producer.rate.ms", "100"));
        int    gradualWin  = Integer.parseInt(props.getProperty("drift.gradual.window", "200"));
        int    recurPeriod = Integer.parseInt(props.getProperty("drift.recurring.period", "150"));

        DatasetLoader loader   = new DatasetLoader(datasetPath);
        DriftInjector injector = new DriftInjector(loader, "sci.space", gradualWin, recurPeriod);

        try (DocumentProducer producer = new DocumentProducer(props, loader, injector)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal — stopping producer.");
                producer.stop();
            }));

            Thread driftScheduler = new Thread(() -> {
                try {
                    Thread.sleep(rateMs * 1000);
                    injector.triggerSudden("talk.politics.guns", 1000);

                    Thread.sleep(rateMs * 1000);
                    injector.triggerGradual("sci.med", 2000);

                    Thread.sleep(rateMs * (gradualWin + 200L));
                    injector.triggerRecurring("comp.graphics", 3200);

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