package com.example;

import java.nio.file.WatchService;
import java.io.Console;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;

public class FileWatcher {
    private static final String directoryToWatch = "/Users/ninoczka/Documents/Master/KafkaApp/Real-Time-Anomaly-detection-Kafka/Data/Raw";
    
    public static void Watch() {
        try {
            System.out.println("Starting file watcher...");
            WatchService watchService = java.nio.file.FileSystems.getDefault().newWatchService();

            Path path = java.nio.file.Paths.get(directoryToWatch);

            WatchKey watchKey = path.register(watchService, java.nio.file.StandardWatchEventKinds.ENTRY_CREATE, java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY, java.nio.file.StandardWatchEventKinds.ENTRY_DELETE);

            while(true)
            {
                for (WatchEvent<?> event : watchKey.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    Path filePath = (Path) event.context();
                    System.out.println("Event kind: " + kind + ". File affected: " + filePath + ".");
                }
            }

        }
        catch (IOException e) {
            // TODO: auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Watching directory: " + directoryToWatch);
    }
}
